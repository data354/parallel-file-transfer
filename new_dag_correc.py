from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook  # Updated import
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import paramiko
import os
import time
from minio import Minio
import threading
import socket
import psycopg2
from psycopg2.extras import RealDictCursor

# Constantes
LOCAL_DIR = "/opt/airflow/data"
LOG_DIR = "/opt/airflow/logs"
BUCKET_NAME = "bronze"
BUCKET_PATH = "datafile"
SSH_CONN_ID = "ssh_connection"
MINIO_CONN_ID = "minio_connect"
DB_CONN_ID = "transfer_db"
MAX_WORKERS = 7

# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'parallel_sftp_to_minio_v23_multiworker__',
    default_args=default_args,
    schedule='@hourly',
    start_date=datetime(2025, 6, 24),
    max_active_runs=2,
    catchup=False
)

class DatabaseManager:
    """Gestionnaire de connexion PostgreSQL persistante"""
    
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.connection = None
        self.cursor = None
        self.lock = threading.Lock()
        self.is_initialized = False
    
    def initialize_connection(self):
        """Initialise la connexion PostgreSQL une seule fois"""
        if self.is_initialized:
            return
            
        with self.lock:
            if self.is_initialized:
                return
                
            try:
                db_conn = BaseHook.get_connection(DB_CONN_ID)
                self.connection = psycopg2.connect(
                    host=db_conn.host,
                    port=db_conn.port or 5432,
                    database=db_conn.schema,
                    user=db_conn.login,
                    password=db_conn.password
                )
                self.connection.autocommit = True
                self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
                self.is_initialized = True
                logging.info(f"[Worker {self.worker_id}] Connexion PostgreSQL initialisée")
                
            except Exception as e:
                logging.error(f"[Worker {self.worker_id}] Erreur lors de l'initialisation PostgreSQL: {str(e)}")
                self.cleanup()
                raise
    
    def init_database(self):
        """Initialise la table de tracking des fichiers"""
        self.initialize_connection()
        
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS file_transfer_log (
                id SERIAL PRIMARY KEY,
                filename VARCHAR(255) UNIQUE NOT NULL,
                worker_id VARCHAR(100) NOT NULL,
                transfer_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transfer_end_time TIMESTAMP,
                file_size_mb DECIMAL(10,2),
                download_duration DECIMAL(10,2),
                upload_duration DECIMAL(10,2),
                total_duration DECIMAL(10,2),
                status VARCHAR(20) DEFAULT 'IN_PROGRESS',
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_filename ON file_transfer_log(filename);
            CREATE INDEX IF NOT EXISTS idx_status ON file_transfer_log(status);
            CREATE INDEX IF NOT EXISTS idx_worker_id ON file_transfer_log(worker_id);
            """
            
            self.cursor.execute(create_table_sql)
            logging.info("Table file_transfer_log initialisée avec succès")
            
        except Exception as e:
            logging.error(f"Erreur lors de l'initialisation de la base de données: {str(e)}")
            raise
    
    
    
    def claim_file_for_processing(self, filename):
        """Tente de réserver un fichier pour le traitement avec gestion des conflits améliorée"""
        self.initialize_connection()
        
        try:
            # Vérification si le fichier est déjà complété
            check_completed_sql = """
            SELECT 1 FROM file_transfer_log 
            WHERE filename = %s AND status = 'COMPLETED'
            LIMIT 1
            """
            self.cursor.execute(check_completed_sql, [filename])
            if self.cursor.fetchone():
                logging.info(f"Fichier {filename} déjà complété - Ignoré")
                return False
            
            # Vérification des entrées IN_PROGRESS existantes
            check_in_progress_sql = """
            SELECT worker_id, transfer_start_time 
            FROM file_transfer_log 
            WHERE filename = %s AND status = 'IN_PROGRESS'
            ORDER BY id DESC 
            LIMIT 1
            """
            self.cursor.execute(check_in_progress_sql, [filename])
            existing = self.cursor.fetchone()
            
            if existing:
                # Si le traitement est en cours depuis plus de 2 minutes, on considère qu'il a échoué
                if existing['transfer_start_time'] < datetime.now() - timedelta(minutes=2):
                    logging.warning(
                        f"Fichier {filename} en statut IN_PROGRESS depuis plus de 2 minutes "
                        f"(worker: {existing['worker_id']}) - Reprise du traitement"
                    )
                    
                    # Marquer l'ancienne tentative comme échouée
                    update_sql = """
                    UPDATE file_transfer_log 
                    SET status = 'FAILED', 
                        error_message = 'Processing timeout',
                        transfer_end_time = CURRENT_TIMESTAMP
                    WHERE filename = %s AND status = 'IN_PROGRESS'
                    """
                    self.cursor.execute(update_sql, [filename])
                    return True
                else:
                    logging.info(
                        f"Fichier {filename} déjà en cours de traitement par "
                        f"worker {existing['worker_id']} - Ignoré"
                    )
                    return False
            
            # Si le fichier n'est ni complété ni en cours, on le réserve
            insert_sql = """
            INSERT INTO file_transfer_log (
                filename, 
                worker_id, 
                status,
                transfer_start_time
            )
            VALUES (%s, %s, 'IN_PROGRESS', CURRENT_TIMESTAMP)
            """
            self.cursor.execute(insert_sql, [filename, self.worker_id])
            logging.info(f"Fichier {filename} réservé pour traitement par worker {self.worker_id}")
            return True
            
        except Exception as e:
            logging.error(f"Erreur lors de la réservation du fichier {filename}: {str(e)}")
            return False



    
    def update_file_status(self, filename, status, result_data=None):
        """Met à jour le statut d'un fichier dans la base de données"""
        self.initialize_connection()
        
        try:
            if status == 'COMPLETED' and result_data:
                update_sql = """
                UPDATE file_transfer_log 
                SET status = %s, 
                    transfer_end_time = CURRENT_TIMESTAMP,
                    file_size_mb = %s,
                    download_duration = %s,
                    upload_duration = %s,
                    total_duration = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE filename = %s AND worker_id = %s
                """
                params = [
                    status,
                    result_data.get('size_mb', 0),
                    result_data.get('download_duration', 0),
                    result_data.get('upload_duration', 0),
                    result_data.get('total_duration', 0),
                    filename,
                    self.worker_id
                ]
            elif status == 'FAILED' and result_data:
                update_sql = """
                UPDATE file_transfer_log 
                SET status = %s, 
                    transfer_end_time = CURRENT_TIMESTAMP,
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE filename = %s AND worker_id = %s
                """
                params = [status, result_data.get('error', ''), filename, self.worker_id]
            else:
                update_sql = """
                UPDATE file_transfer_log 
                SET status = %s, 
                    updated_at = CURRENT_TIMESTAMP
                WHERE filename = %s AND worker_id = %s
                """
                params = [status, filename, self.worker_id]
            
            self.cursor.execute(update_sql, params)
            logging.info(f"Statut du fichier {filename} mis à jour: {status}")
            
        except Exception as e:
            logging.error(f"Erreur lors de la mise à jour du statut pour {filename}: {str(e)}")
    
    def get_processed_files(self):
        """Récupère la liste des fichiers déjà traités avec succès"""
        self.initialize_connection()
        
        try:
            query = """
            SELECT filename FROM file_transfer_log 
            WHERE status = 'COMPLETED'
            """
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            return set(row['filename'] for row in result)
            
        except Exception as e:
            logging.error(f"Erreur lors de la récupération des fichiers traités: {str(e)}")
            return set()
    

    def get_active_workers_info(self):
        """Récupère les informations sur les workers actifs"""
        self.initialize_connection()
        
        try:
            active_workers_query = """
            SELECT DISTINCT worker_id, COUNT(*) as files_count, MAX(transfer_start_time) as last_activity
            FROM file_transfer_log 
            WHERE transfer_start_time > NOW() - INTERVAL '1 hour'
            GROUP BY worker_id
            ORDER BY last_activity DESC
            """
            
            self.cursor.execute(active_workers_query)
            return self.cursor.fetchall()
            
        except Exception as e:
            logging.error(f"Erreur lors du diagnostic des workers: {str(e)}")
            return []



    def get_failed_files_for_retry(self, max_retries=3):
        """Récupère les fichiers en échec qui peuvent être retentés"""
        self.initialize_connection()
        
        try:
            query = """
            SELECT filename, COUNT(*) as retry_count 
            FROM file_transfer_log 
            WHERE status = 'FAILED' 
            GROUP BY filename 
            HAVING COUNT(*) < %s
            """
            self.cursor.execute(query, [max_retries])
            result = self.cursor.fetchall()
            return set(row['filename'] for row in result)
            
        except Exception as e:
            logging.error(f"Erreur lors de la récupération des fichiers à retenter: {str(e)}")
            return set()
    
    def cleanup_stale_locks(self, max_age_minutes=3):
        """Nettoie les verrous obsolètes en les marquant comme FAILED"""
        self.initialize_connection()
        
        try:
            select_sql = """
            SELECT filename, worker_id FROM file_transfer_log 
            WHERE status = 'IN_PROGRESS' 
            AND transfer_start_time < NOW() - INTERVAL '%s minutes'
            """
            self.cursor.execute(select_sql, [max_age_minutes])
            stale_files = self.cursor.fetchall()
            
            if stale_files:
                logging.warning(f"Nettoyage de {len(stale_files)} fichiers en attente trop longtemps:")
                for file_info in stale_files:
                    logging.warning(f"  - {file_info['filename']} (worker: {file_info['worker_id']})")
            
            cleanup_sql = """
            UPDATE file_transfer_log 
            SET status = 'FAILED', 
                error_message = 'Timeout - Process interrupted or took too long',
                transfer_end_time = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE status = 'IN_PROGRESS' 
            AND transfer_start_time < NOW() - INTERVAL '%s minutes'
            """
            self.cursor.execute(cleanup_sql, [max_age_minutes])
            logging.info(f"Nettoyage des verrous obsolètes effectué par {self.worker_id}")
            
        except Exception as e:
            logging.error(f"Erreur lors du nettoyage des verrous: {str(e)}")
    
    def get_retry_statistics(self):
        """Obtient les statistiques de retry pour le monitoring"""
        self.initialize_connection()
        
        try:
            stats_query = """
            SELECT 
                COUNT(DISTINCT filename) as unique_files,
                COUNT(*) as total_attempts,
                COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as successful_attempts,
                COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_attempts,
                COUNT(CASE WHEN status = 'IN_PROGRESS' THEN 1 END) as in_progress_attempts
            FROM file_transfer_log
            WHERE created_at > NOW() - INTERVAL '24 hours'
            """
            self.cursor.execute(stats_query)
            return self.cursor.fetchone()
            
        except Exception as e:
            logging.error(f"Erreur lors de la récupération des statistiques: {str(e)}")
            return None
    
    def cleanup(self):
        """Ferme proprement la connexion PostgreSQL"""
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
            self.is_initialized = False
            logging.info(f"[Worker {self.worker_id}] Connexion PostgreSQL fermée")
        except Exception as e:
            logging.error(f"[Worker {self.worker_id}] Erreur lors de la fermeture PostgreSQL: {str(e)}")
    
    def __del__(self):
        self.cleanup()

def get_worker_id():
    """Génère un ID unique pour le worker"""
    hostname = socket.gethostname()
    pid = os.getpid()
    return f"{hostname}_{pid}"

class PersistentConnectionHandler:
    """Gestionnaire de connexions persistantes par thread"""
    
    def __init__(self, worker_id, db_manager):
        self.ssh_client = None
        self.sftp_client = None
        self.minio_client = None
        self.remote_dir = None
        self.thread_id = threading.get_ident()
        self.worker_id = worker_id
        self.db_manager = db_manager
        self.is_initialized = False
        self.lock = threading.Lock()
        
    def initialize_connections(self):
        """Initialise les connexions une seule fois par thread"""
        if self.is_initialized:
            return
            
        with self.lock:
            if self.is_initialized:
                return
                
            try:
                thread_name = threading.current_thread().name
                logging.info(f"[Worker {self.worker_id}][Thread {thread_name}] Initialisation des connexions...")
                
                ssh_conn = BaseHook.get_connection(SSH_CONN_ID)
                minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
                
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
                
                minio_extra = minio_conn.extra_dejson or {}
                endpoint_url = minio_extra.get("endpoint_url", "")
                
                if not endpoint_url:
                    raise ValueError(f"endpoint_url manquant dans la configuration MinIO {MINIO_CONN_ID}")
                
                endpoint_url = endpoint_url.replace("http://", "").replace("https://", "")
                
                self.minio_client = Minio(
                    endpoint_url,
                    access_key=minio_conn.login,
                    secret_key=minio_conn.password,
                    secure=False
                )
                
                self.minio_client.bucket_exists(BUCKET_NAME)
                self.is_initialized = True
                logging.info(f"[Worker {self.worker_id}][Thread {thread_name}] Connexions initialisées avec succès")
                
            except Exception as e:
                logging.error(f"[Worker {self.worker_id}][Thread {thread_name}] Erreur lors de l'initialisation: {str(e)}")
                self.cleanup()
                raise
    
    def transfer_file(self, filename):
        """Transfère un fichier en utilisant les connexions persistantes"""
        thread_name = threading.current_thread().name
        start_time = time.time()
        local_path = None
        
        try:
            if not self.db_manager.claim_file_for_processing(filename):
                return {
                    'filename': filename,
                    'skipped': True,
                    'reason': 'Already being processed or completed'
                }
            
            self.initialize_connections()
            
            logging.info(f"[Worker {self.worker_id}][Thread {thread_name}] Début du transfert de {filename}")
            
            os.makedirs(LOCAL_DIR, exist_ok=True)
            local_path = os.path.join(LOCAL_DIR, f"{filename}_{self.worker_id}_{self.thread_id}")
            remote_path = os.path.join(self.remote_dir, filename)
            
            download_start = time.time()
            self.sftp_client.get(remote_path, local_path)
            download_duration = time.time() - download_start
            
            file_size = os.path.getsize(local_path) / (1024 * 1024)
            download_speed = file_size / download_duration if download_duration > 0 else 0
            
            logging.info(f"[Worker {self.worker_id}][Thread {thread_name}] [{filename}] Téléchargement terminé - Size: {file_size:.2f} MB - Duration: {download_duration:.2f}s - Speed: {download_speed:.2f} MB/s")
            
            upload_start = time.time()
            self.minio_client.fput_object(
                BUCKET_NAME, 
                os.path.join(BUCKET_PATH, filename), 
                local_path
            )
            upload_duration = time.time() - upload_start
            upload_speed = file_size / upload_duration if upload_duration > 0 else 0
            
            os.remove(local_path)
            local_path = None
            
            total_duration = time.time() - start_time
            
            result = {
                'filename': filename,
                'size_mb': file_size,
                'download_duration': download_duration,
                'upload_duration': upload_duration,
                'total_duration': total_duration,
                'success': True
            }
            
            self.db_manager.update_file_status(filename, 'COMPLETED', result)
            
            logging.info(f"[Worker {self.worker_id}][Thread {thread_name}] [{filename}] Transfert complet - Upload: {upload_duration:.2f}s ({upload_speed:.2f} MB/s) - Total: {total_duration:.2f}s")
            
            return result
            
        except Exception as e:
            if local_path and os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except:
                    pass
            
            error_result = {
                'filename': filename,
                'error': str(e),
                'success': False
            }
            
            self.db_manager.update_file_status(filename, 'FAILED', error_result)
            
            logging.error(f"[Worker {self.worker_id}][Thread {thread_name}] Erreur lors du transfert de {filename}: {str(e)}")
            return error_result
    
    def cleanup(self):
        """Ferme proprement les connexions"""
        thread_name = threading.current_thread().name
        try:
            if self.sftp_client:
                self.sftp_client.close()
                self.sftp_client = None
            if self.ssh_client:
                self.ssh_client.close()
                self.ssh_client = None
            self.minio_client = None
            self.is_initialized = False
            logging.info(f"[Worker {self.worker_id}][Thread {thread_name}] Connexions fermées")
        except Exception as e:
            logging.error(f"[Worker {self.worker_id}][Thread {thread_name}] Erreur lors de la fermeture: {str(e)}")
    
    def __del__(self):
        self.cleanup()

_thread_local = threading.local()

def get_connection_handler(worker_id, db_manager):
    """Récupère ou crée le gestionnaire de connexions pour le thread actuel"""
    if not hasattr(_thread_local, 'handler'):
        _thread_local.handler = PersistentConnectionHandler(worker_id, db_manager)
    return _thread_local.handler

def transfer_file_with_persistent_connections(filename, worker_id, db_manager):
    """Wrapper pour le transfert avec connexions persistantes"""
    handler = get_connection_handler(worker_id, db_manager)
    return handler.transfer_file(filename)



def run_transfer():
    worker_id = get_worker_id()
    db_manager = None
    
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"collect_{worker_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    try:
        logging.info(f"=== DÉMARRAGE WORKER {worker_id} ===")
        logging.info(f"PID: {os.getpid()}")
        logging.info(f"Hostname: {socket.gethostname()}")
        logging.info(f"DAG ID: {dag.dag_id}")
        logging.info(f"Task ID: run_transfer_task")
        
        db_manager = DatabaseManager(worker_id)
        db_manager.init_database()
        db_manager.cleanup_stale_locks(max_age_minutes=3)
        
        retry_stats = db_manager.get_retry_statistics()
        if retry_stats:
            logging.info(f"=== STATISTIQUES RETRY (24h) ===")
            logging.info(f"Fichiers uniques: {retry_stats['unique_files']}")
            logging.info(f"Tentatives totales: {retry_stats['total_attempts']}")
            logging.info(f"Tentatives réussies: {retry_stats['successful_attempts']}")
            logging.info(f"Tentatives échouées: {retry_stats['failed_attempts']}")
            logging.info(f"Tentatives en cours: {retry_stats['in_progress_attempts']}")
        
        active_workers = db_manager.get_active_workers_info()
        logging.info(f"Workers actifs dans la dernière heure: {len(active_workers)}")
        for worker_info in active_workers:
            logging.info(f"  - Worker {worker_info['worker_id']}: {worker_info['files_count']} fichiers, dernière activité: {worker_info['last_activity']}")
        
        minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
        minio_extra = minio_conn.extra_dejson or {}
        if not minio_extra.get("endpoint_url"):
            raise ValueError(f"Configuration MinIO invalide: endpoint_url manquant dans la connexion {MINIO_CONN_ID}")
        
        processed_files = db_manager.get_processed_files()
        failed_files_to_retry = db_manager.get_failed_files_for_retry(max_retries=3)
        
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
        new_files = list((all_files - processed_files) | failed_files_to_retry)
        
        sftp.close()
        ssh.close()
        
        logging.info(f"Fichiers totaux sur le serveur: {len(all_files)}")
        logging.info(f"Fichiers déjà traités avec succès: {len(processed_files)}")
        logging.info(f"Fichiers en échec à retenter: {len(failed_files_to_retry)}")
        logging.info(f"Fichiers à traiter (nouveaux + retry): {len(new_files)}")
        
        if failed_files_to_retry:
            logging.info(f"Fichiers en échec qui seront retentés: {list(failed_files_to_retry)}")
        
        if new_files:
            logging.info(f"[Worker {worker_id}] Fichiers à traiter: {len(new_files)}")
            
            results = []
            skipped_files = []
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                try:
                    future_to_filename = {
                        executor.submit(transfer_file_with_persistent_connections, filename, worker_id, db_manager): filename
                        for filename in new_files
                    }
                    
                    for future in as_completed(future_to_filename):
                        filename = future_to_filename[future]
                        try:
                            result = future.result()
                            if result.get('skipped'):
                                skipped_files.append(filename)
                            results.append(result)
                        except Exception as e:
                            logging.error(f"[Worker {worker_id}] Erreur lors du traitement de {filename}: {str(e)}")
                            results.append({
                                'filename': filename,
                                'error': str(e),
                                'success': False
                            })
                
                finally:
                    # Phase de récupération des fichiers ignorés
                    if skipped_files:
                        logging.info(f"[Worker {worker_id}] Début de la phase de récupération pour {len(skipped_files)} fichiers ignorés")
                        
                        for filename in skipped_files:
                            try:
                                logging.info(f"[Worker {worker_id}] Vérification du statut de {filename}")
                                
                                db_manager.initialize_connection()
                                status_query = """
                                SELECT status, transfer_start_time
                                FROM file_transfer_log
                                WHERE filename = %s
                                ORDER BY id DESC
                                LIMIT 1
                                """
                                db_manager.cursor.execute(status_query, [filename])
                                record = db_manager.cursor.fetchone()
                                
                                if not record or record['status'] != 'COMPLETED':
                                    logging.info(f"[Worker {worker_id}] Tentative de récupération pour {filename}")
                                    result = transfer_file_with_persistent_connections(filename, worker_id, db_manager)
                                    results.append(result)
                                else:
                                    logging.info(f"[Worker {worker_id}] {filename} déjà complété par un autre worker")
                                    results.append({
                                        'filename': filename,
                                        'skipped': True,
                                        'reason': 'Already completed by another worker'
                                    })
                            except Exception as e:
                                logging.error(f"[Worker {worker_id}] Erreur lors de la vérification de {filename}: {str(e)}")
                            finally:
                                db_manager.cleanup()
            
            # Analyse des résultats
            successful_files = []
            failed_files = []
            total_size = 0
            total_time = 0
            
            for result in results:
                if result.get('success'):
                    successful_files.append(result['filename'])
                    total_size += result.get('size_mb', 0)
                    total_time += result.get('total_duration', 0)
                elif not result.get('skipped'):
                    failed_files.append(result['filename'])
            
            avg_speed = total_size / total_time if total_time > 0 else 0
            logging.info(f"=== RÉSUMÉ WORKER {worker_id} ===")
            logging.info(f"Fichiers candidats: {len(new_files)}")
            logging.info(f"Traités avec succès: {len(successful_files)}")
            logging.info(f"Échoués: {len(failed_files)}")
            logging.info(f"Taille totale traitée: {total_size:.2f} MB")
            logging.info(f"Temps total: {total_time:.2f}s")
            logging.info(f"Vitesse moyenne: {avg_speed:.2f} MB/s")
            
            if failed_files:
                logging.error(f"Fichiers en échec: {failed_files}")
                raise Exception(f"Échec du transfert de {len(failed_files)} fichiers")
                
        else:
            logging.info(f"[Worker {worker_id}] Aucun fichier à transférer")
            
    except Exception as e:
        logging.error(f"[Worker {worker_id}] Erreur dans run_transfer: {str(e)}")
        raise
    finally:
        if db_manager:
            try:
                db_manager.initialize_connection()
                
                # Dernière vérification des fichiers potentiellement restants
                pending_query = """
                SELECT filename
                FROM file_transfer_log
                WHERE status = 'IN_PROGRESS'
                AND transfer_start_time < NOW() - INTERVAL '2 minutes'
                ORDER BY transfer_start_time
                """
                db_manager.cursor.execute(pending_query)
                pending_files = [row['filename'] for row in db_manager.cursor.fetchall()]
                
                if pending_files:
                    logging.warning(f"[Worker {worker_id}] Tentative de récupération finale pour {len(pending_files)} fichiers restants")
                    
                    for filename in pending_files:
                        try:
                            logging.info(f"[Worker {worker_id}] Traitement final de {filename}")
                            result = transfer_file_with_persistent_connections(filename, worker_id, db_manager)
                            if result.get('success'):
                                logging.info(f"[Worker {worker_id}] {filename} transféré avec succès lors de la phase finale")
                            else:
                                logging.error(f"[Worker {worker_id}] Échec final pour {filename}: {result.get('error', 'Unknown error')}")
                        except Exception as e:
                            logging.error(f"[Worker {worker_id}] Erreur critique lors du traitement final de {filename}: {str(e)}")
                
                # Nettoyage final
                cleanup_sql = """
                UPDATE file_transfer_log
                SET status = 'FAILED',
                    error_message = 'Worker interrupted or crashed',
                    transfer_end_time = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE worker_id = %s AND status = 'IN_PROGRESS'
                """
                db_manager.cursor.execute(cleanup_sql, [worker_id])
                logging.info(f"[Worker {worker_id}] Nettoyage final terminé")
                
            except Exception as cleanup_error:
                logging.error(f"[Worker {worker_id}] Erreur lors du nettoyage final: {str(cleanup_error)}")
            finally:
                db_manager.cleanup()

transfer_task = PythonOperator(
    task_id='run_transfer_task',
    python_callable=run_transfer,
    dag=dag
)