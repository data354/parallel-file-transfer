from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Connection
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from multiprocessing import Pool
import logging
import paramiko
import os
import time
import pickle
from minio import Minio

# Constantes
LOCAL_DIR = "/tmp/data"
TRACKER_FILE = "/home/ubuntu/collected_files.pkl"
LOG_DIR = "/home/ubuntu/logs"
BUCKET_NAME = "bronze"
BUCKET_PATH = "datafile"
SSH_CONN_ID = "ssh_connection"
MINIO_CONN_ID = "minio_connect"

# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'parallel_sftp_to_minio',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2025, 6, 24),
    catchup=False
)

def load_collected_files():
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE, "rb") as f:
            return pickle.load(f)
    return set()

def save_collected_files(collected_files):
    with open(TRACKER_FILE, "wb") as f:
        pickle.dump(collected_files, f)

class WorkerProcess:
    """Classe pour maintenir les connexions ouvertes dans chaque processus worker"""
    
    def __init__(self):
        self.ssh_client = None
        self.sftp_client = None
        self.minio_client = None
        self.pid = os.getpid()
        self._initialize_connections()
    
    def _initialize_connections(self):
        """Initialise les connexions SSH et MinIO pour ce processus"""
        try:
            # Récupération des connexions Airflow
            ssh_conn = BaseHook.get_connection(SSH_CONN_ID)
            minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
            
            # Connexion SSH persistante
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                ssh_conn.host,
                port=int(ssh_conn.port or 22),
                username=ssh_conn.login,
                key_filename=ssh_conn.extra_dejson.get("key_file")
            )
            self.sftp_client = self.ssh_client.open_sftp()
            self.remote_dir = ssh_conn.extra_dejson.get("remote_dir", "/data")
            
            logging.info(f"[PID {self.pid}] Connexion SSH établie")
            
            # Connexion MinIO persistante
            minio_extra = minio_conn.extra_dejson or {}
            endpoint_url = minio_extra.get("endpoint_url", "")
            
            if not endpoint_url:
                raise ValueError(f"endpoint_url manquant dans la configuration MinIO {MINIO_CONN_ID}")
            
            # Extraction de l'endpoint depuis l'URL
            endpoint_url = endpoint_url.replace("http://", "").replace("https://", "")
            
            self.minio_client = Minio(
                endpoint_url,
                access_key=minio_conn.login,
                secret_key=minio_conn.password,
                secure=False
            )
            
            logging.info(f"[PID {self.pid}] Connexion MinIO établie vers {endpoint_url}")
            
        except Exception as e:
            logging.error(f"[PID {self.pid}] Erreur lors de l'initialisation des connexions: {str(e)}")
            self.cleanup()
            raise
    
    def transfer_file(self, filename):
        """Transfère un fichier en utilisant les connexions persistantes"""
        start_time = time.time()
        local_path = None
        
        try:
            logging.info(f"[PID {self.pid}] Début du transfert de {filename}")
            
            # Création du répertoire local
            os.makedirs(LOCAL_DIR, exist_ok=True)
            local_path = os.path.join(LOCAL_DIR, filename)
            remote_path = os.path.join(self.remote_dir, filename)
            
            # Téléchargement SFTP
            download_start = time.time()
            self.sftp_client.get(remote_path, local_path)
            download_duration = time.time() - download_start
            
            file_size = os.path.getsize(local_path) / (1024 * 1024)  # MB
            download_speed = file_size / download_duration if download_duration > 0 else 0
            
            logging.info(f"[PID {self.pid}] [{filename}] Téléchargement terminé - Size: {file_size:.2f} MB - Duration: {download_duration:.2f}s - Speed: {download_speed:.2f} MB/s")
            
            # Upload vers MinIO
            upload_start = time.time()
            self.minio_client.fput_object(BUCKET_NAME, os.path.join(BUCKET_PATH, filename), local_path)
            upload_duration = time.time() - upload_start
            upload_speed = file_size / upload_duration if upload_duration > 0 else 0
            
            # Nettoyage du fichier local
            os.remove(local_path)
            
            total_duration = time.time() - start_time
            
            logging.info(f"[PID {self.pid}] [{filename}] Transfert complet - Upload: {upload_duration:.2f}s ({upload_speed:.2f} MB/s) - Total: {total_duration:.2f}s")
            
            return {
                'filename': filename,
                'size_mb': file_size,
                'download_duration': download_duration,
                'upload_duration': upload_duration,
                'total_duration': total_duration,
                'success': True
            }
            
        except Exception as e:
            # Nettoyage en cas d'erreur
            if local_path and os.path.exists(local_path):
                os.remove(local_path)
            
            logging.error(f"[PID {self.pid}] Erreur lors du transfert de {filename}: {str(e)}")
            return {
                'filename': filename,
                'error': str(e),
                'success': False
            }
    
    def cleanup(self):
        """Ferme proprement les connexions"""
        try:
            if self.sftp_client:
                self.sftp_client.close()
                self.sftp_client = None
            if self.ssh_client:
                self.ssh_client.close()
                self.ssh_client = None
            logging.info(f"[PID {self.pid}] Connexions fermées")
        except Exception as e:
            logging.error(f"[PID {self.pid}] Erreur lors de la fermeture des connexions: {str(e)}")
    
    def __del__(self):
        """Destructeur pour s'assurer que les connexions sont fermées"""
        self.cleanup()

# Instance globale du worker pour chaque processus
worker = None

def init_worker():
    """Fonction d'initialisation appelée une fois par processus worker"""
    global worker
    worker = WorkerProcess()

def transfer_file_wrapper(filename):
    """Wrapper pour le transfert de fichier utilisant le worker global"""
    global worker
    if worker is None:
        # Fallback si l'initialisation a échoué
        init_worker()
    
    try:
        return worker.transfer_file(filename)
    except Exception as e:
        # En cas d'erreur, on nettoie et on réessaie une fois
        logging.error(f"[PID {os.getpid()}] Erreur dans transfer_file_wrapper, tentative de reconnexion: {str(e)}")
        if worker:
            worker.cleanup()
        init_worker()
        return worker.transfer_file(filename)

def run_transfer():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"collect_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # Configuration du logging
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - PID %(process)d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    try:
        # Vérification de la connexion MinIO avant de commencer
        minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
        minio_extra = minio_conn.extra_dejson or {}
        if not minio_extra.get("endpoint_url"):
            raise ValueError(f"Configuration MinIO invalide: endpoint_url manquant dans la connexion {MINIO_CONN_ID}")
        
        collected_files = load_collected_files()
        
        # Connexion SSH temporaire pour lister les fichiers
        ssh_conn = BaseHook.get_connection(SSH_CONN_ID)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            ssh_conn.host, 
            port=int(ssh_conn.port or 22), 
            username=ssh_conn.login, 
            key_filename=ssh_conn.extra_dejson.get("key_file")
        )
        sftp = ssh.open_sftp()
        
        all_files = set(sftp.listdir(ssh_conn.extra_dejson.get("remote_dir", "/data")))
        new_files = list(all_files - collected_files)
        
        sftp.close()
        ssh.close()
        
        if new_files:
            logging.info(f"Nouveaux fichiers trouvés: {len(new_files)}")
            
            # Pool de processus avec initialisation seulement
            with Pool(processes=4, initializer=init_worker) as pool:
                results = pool.map(transfer_file_wrapper, new_files)
            
            # Analyse des résultats
            successful_files = []
            failed_files = []
            total_size = 0
            total_time = 0
            
            for result in results:
                if result['success']:
                    successful_files.append(result['filename'])
                    total_size += result['size_mb']
                    total_time += result['total_duration']
                else:
                    failed_files.append(result['filename'])
            
            # Mise à jour du tracker seulement pour les fichiers réussis
            if successful_files:
                collected_files.update(successful_files)
                save_collected_files(collected_files)
            
            # Statistiques finales
            avg_speed = total_size / total_time if total_time > 0 else 0
            logging.info(f"=== RÉSUMÉ ===")
            logging.info(f"Fichiers traités: {len(new_files)}")
            logging.info(f"Réussis: {len(successful_files)}")
            logging.info(f"Échoués: {len(failed_files)}")
            logging.info(f"Taille totale: {total_size:.2f} MB")
            logging.info(f"Temps total: {total_time:.2f}s")
            logging.info(f"Vitesse moyenne: {avg_speed:.2f} MB/s")
            
            if failed_files:
                logging.error(f"Fichiers en échec: {failed_files}")
                
        else:
            logging.info("Aucun nouveau fichier à transférer")
            
    except Exception as e:
        logging.error(f"Erreur dans run_transfer: {str(e)}")
        raise

transfer_task = PythonOperator(
    task_id='run_transfer_task',
    python_callable=run_transfer,
    dag=dag
)