# sftp_to_minio/database_manager.py
import logging
import threading
import psycopg2
from psycopg2.extras import RealDictCursor
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from dag_config import DB_CONN_ID

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
            check_completed_sql = """
            SELECT 1 FROM file_transfer_log 
            WHERE filename = %s AND status = 'COMPLETED'
            LIMIT 1
            """
            self.cursor.execute(check_completed_sql, [filename])
            if self.cursor.fetchone():
                logging.info(f"Fichier {filename} déjà complété - Ignoré")
                return False
            
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
                if existing['transfer_start_time'] < datetime.now() - timedelta(minutes=4):
                    logging.warning(
                        f"Fichier {filename} en statut IN_PROGRESS depuis plus de 2 minutes "
                        f"(worker: {existing['worker_id']}) - Reprise du traitement"
                    )
                    
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