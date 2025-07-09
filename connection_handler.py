# sftp_to_minio/connection_handler.py
import threading
import paramiko
import os
import time
import logging
from minio import Minio
from airflow.hooks.base import BaseHook
from dag_config import LOCAL_DIR, BUCKET_NAME, BUCKET_PATH, SSH_CONN_ID, MINIO_CONN_ID
from database_manager import DatabaseManager

class PersistentConnectionHandler:
    """Gestionnaire de connexions persistantes par thread"""
    
    def __init__(self, worker_id, db_manager: DatabaseManager):
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

def get_connection_handler(worker_id, db_manager: DatabaseManager):
    """Récupère ou crée le gestionnaire de connexions pour le thread actuel"""
    if not hasattr(_thread_local, 'handler'):
        _thread_local.handler = PersistentConnectionHandler(worker_id, db_manager)
    return _thread_local.handler

def transfer_file_with_persistent_connections(filename, worker_id, db_manager: DatabaseManager):
    """Wrapper pour le transfert avec connexions persistantes"""
    handler = get_connection_handler(worker_id, db_manager)
    return handler.transfer_file(filename)