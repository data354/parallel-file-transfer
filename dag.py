# main_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import paramiko
import os
import socket
from datetime import datetime
from dag_config import default_args, LOG_DIR, MINIO_CONN_ID, SSH_CONN_ID, MAX_WORKERS
from database_manager import DatabaseManager
from connection_handler import transfer_file_with_persistent_connections

def get_worker_id():
    """Génère un ID unique pour le worker"""
    hostname = socket.gethostname()
    pid = os.getpid()
    return f"{hostname}_{pid}"

def run_transfer():
    worker_id = get_worker_id()
    db_manager = None
    
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"collect_{worker_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # Configuration du logging
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
        
        # Initialisation du gestionnaire de base de données
        db_manager = DatabaseManager(worker_id)
        
        # Initialisation de la base de données
        db_manager.init_database()
        
        # Nettoyage des verrous obsolètes
        db_manager.cleanup_stale_locks()
        
        # Diagnostic des workers actifs
        active_workers = db_manager.get_active_workers_info()
        logging.info(f"Workers actifs dans la dernière heure: {len(active_workers)}")
        for worker_info in active_workers:
            logging.info(f"  - Worker {worker_info['worker_id']}: {worker_info['files_count']} fichiers, dernière activité: {worker_info['last_activity']}")
        
        # Vérification de la connexion MinIO
        minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
        minio_extra = minio_conn.extra_dejson or {}
        if not minio_extra.get("endpoint_url"):
            raise ValueError(f"Configuration MinIO invalide: endpoint_url manquant dans la connexion {MINIO_CONN_ID}")
        
        # Récupération des fichiers déjà traités
        processed_files = db_manager.get_processed_files()
        logging.info(f"Fichiers déjà traités: {len(processed_files)}")
        
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
        new_files = list(all_files - processed_files)
        
        sftp.close()
        ssh.close()
        
        logging.info(f"Fichiers totaux sur le serveur: {len(all_files)}")
        logging.info(f"Nouveaux fichiers à traiter: {len(new_files)}")
        
        if new_files:
            logging.info(f"[Worker {worker_id}] Nouveaux fichiers trouvés: {len(new_files)}")
            
            # Utilisation de ThreadPoolExecutor avec connexions persistantes
            results = []
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                try:
                    # Soumission des tâches
                    future_to_filename = {
                        executor.submit(transfer_file_with_persistent_connections, filename, worker_id, db_manager): filename 
                        for filename in new_files
                    }
                    
                    # Collecte des résultats
                    for future in as_completed(future_to_filename):
                        filename = future_to_filename[future]
                        try:
                            result = future.result()
                            results.append(result)
                        except Exception as e:
                            logging.error(f"[Worker {worker_id}] Erreur lors du traitement de {filename}: {str(e)}")
                            results.append({
                                'filename': filename,
                                'error': str(e),
                                'success': False
                            })
                
                finally:
                    # Nettoyage automatique des connexions lors de la fin des threads
                    pass
            
            # Analyse des résultats
            successful_files = []
            failed_files = []
            skipped_files = []
            total_size = 0
            total_time = 0
            
            for result in results:
                if result.get('skipped'):
                    skipped_files.append(result['filename'])
                elif result.get('success'):
                    successful_files.append(result['filename'])
                    total_size += result['size_mb']
                    total_time += result['total_duration']
                else:
                    failed_files.append(result['filename'])
            
            # Statistiques finales
            avg_speed = total_size / total_time if total_time > 0 else 0
            logging.info(f"=== RÉSUMÉ WORKER {worker_id} ===")
            logging.info(f"Fichiers candidats: {len(new_files)}")
            logging.info(f"Traités avec succès: {len(successful_files)}")
            logging.info(f"Ignorés (déjà en cours): {len(skipped_files)}")
            logging.info(f"Échoués: {len(failed_files)}")
            logging.info(f"Taille totale traitée: {total_size:.2f} MB")
            logging.info(f"Temps total: {total_time:.2f}s")
            logging.info(f"Vitesse moyenne: {avg_speed:.2f} MB/s")
            
            if failed_files:
                logging.error(f"Fichiers en échec: {failed_files}")
            if skipped_files:
                logging.info(f"Fichiers ignorés: {skipped_files}")
                
        else:
            logging.info(f"[Worker {worker_id}] Aucun nouveau fichier à transférer")
            
    except Exception as e:
        logging.error(f"[Worker {worker_id}] Erreur dans run_transfer: {str(e)}")
        raise
    finally:
        # Fermeture de la connexion PostgreSQL
        if db_manager:
            db_manager.cleanup()

dag = DAG(
    'parallel_sftp_to_minio_v22_multiworker_v_4',
    default_args=default_args,
    schedule='@hourly' ,
    start_date=datetime(2025, 6, 24),
    max_active_runs=2,
    catchup=False
)

transfer_task = PythonOperator(
    task_id='run_transfer_task',
    python_callable=run_transfer,
    dag=dag
)