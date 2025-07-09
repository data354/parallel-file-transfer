# sftp_to_minio/main_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
# Correction de l'import Dataset pour les versions récentes d'Airflow
try:
    from airflow.datasets import Dataset
except ImportError:
    # Fallback pour les versions plus anciennes
    from airflow.models.dataset import Dataset
    
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import socket
import os
from datetime import datetime
import paramiko
from config import default_args, LOCAL_DIR, LOG_DIR, SSH_CONN_ID, MINIO_CONN_ID, MAX_WORKERS, BUCKET_NAME, BUCKET_PATH
from database_manager import DatabaseManager
from connection_handler import transfer_file_with_persistent_connections
from airflow.hooks.base import BaseHook

# Define the dataset pointing to the bronze bucket in MinIO
BRONZE_DATASET = Dataset(f"s3://{BUCKET_NAME}/{BUCKET_PATH}")

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
            
            # Log dataset creation if there are successful transfers
            if successful_files:
                logging.info(f"[Worker {worker_id}] Dataset created: {BRONZE_DATASET.uri}")
            
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

def hello_task():
    """Tâche qui affiche un message hello quand le dataset est mis à jour"""
    logging.info("Hello! Le dataset bronze a été mis à jour avec de nouveaux fichiers!")
    logging.info(f"Dataset URI: {BRONZE_DATASET.uri}")
    print("Hello! Le dataset bronze a été mis à jour avec de nouveaux fichiers!")
    print(f"Dataset URI: {BRONZE_DATASET.uri}")

dag = DAG(
    'parallel_sftp_to_minio_v23_multiworker__m',
    default_args=default_args,
    schedule='@hourly',
    start_date=datetime(2025, 6, 24),
    max_active_runs=2,
    catchup=False
)

transfer_task = PythonOperator(
    task_id='run_transfer_task',
    python_callable=run_transfer,
    outlets=[BRONZE_DATASET],
    dag=dag
)

# DAG séparé qui se déclenche quand le dataset est mis à jour
hello_dag = DAG(
    'hello_on_dataset_update',
    default_args=default_args,
    schedule=[BRONZE_DATASET],  # Se déclenche quand le dataset est mis à jour
    start_date=datetime(2025, 6, 24),
    max_active_runs=1,
    catchup=False
)

hello_task_operator = PythonOperator(
    task_id='hello_task',
    python_callable=hello_task,
    dag=hello_dag  
)