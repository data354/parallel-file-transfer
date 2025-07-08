
# ⚙️ Système de Transfert Parallèle SFTP vers MinIO

Ce projet implémente un système robuste de transfert parallèle de fichiers depuis un serveur SFTP vers un stockage MinIO, orchestré via Apache Airflow. Il est conçu pour être **scalable**, **fiable**, et **résilient**, même en environnement distribué multi-workers.

---

## 🧱 Architecture

Le projet est modulaire pour garantir sa maintenabilité et son évolutivité.

### 📁 Structure des fichiers

```
├── dag_config.py             # Configuration générale
├── database_manager.py       # Gestion de la base PostgreSQL
├── connection_handler.py     # Connexions persistantes (SSH, MinIO, DB)
├── dag.py                    # DAG Airflow principal
└── README.md                 # Documentation du projet
```

---

## 🚀 Fonctionnalités

### ✅ Transfert parallèle
- Traitement simultané de fichiers avec `ThreadPoolExecutor`
- Connexions persistantes (SFTP et MinIO) partagées entre threads
- Gestion automatique de la mémoire et des ressources

### 🤝 Coordination multi-workers
- Verrouillage distribué via PostgreSQL
- Évite le traitement redondant de fichiers
- Nettoyage automatique des verrous obsolètes

### 📊 Monitoring & Logging
- Logs détaillés par worker et thread
- Statistiques de transfert (taille, durée, débit)
- Débogage simplifié via Airflow UI et logs locaux

### 🛡️ Robustesse
- Gestion fine des erreurs
- Mécanismes de récupération
- Isolation entre threads et workers

---

## ⚙️ Configuration

### 🔌 Connexions Airflow requises

1. **ssh_connection**
```json
{
  "Connection Id": "ssh_connection",
  "Connection Type": "SSH",
  "host": "server.host",
  "port": 22,
  "login": "username",
  "extra": {
    "key_file": "/home/ubuntu/.ssh/id_rsa"
  }
}
```

2. **minio_connect**
```json
{
  "Connection Id": "minio_connect",
  "Connection Type": "Amazon Web Services",
  "AWS Access Key ID": "minio-key",
  "AWS Secret Access Key": "minio-secret",
  "extra": {
    "endpoint_url": "http://minio.host:9000"
  }
}
```

3. **transfer_db**
```json
{
  "Connection Id": "transfer_db",
  "Connection Type": "Postgres",
  "host": "postgres.host",
  "port": 5432,
  "login": "username",
  "password": "password",
  "database": "database_name"
}
```

### 🔧 Variables dans `dag_config.py`
- `MAX_WORKERS` : Nombre de threads par worker (défaut: 4)
- `LOCAL_DIR` : Répertoire temporaire local
- `BUCKET_NAME` : Nom du bucket MinIO
- `MAX_STALE_LOCK_MINUTES` : Durée avant expiration d’un verrou (défaut: 30)

---

## 📦 Utilisation

Le DAG est déclenché automatiquement (`@hourly` par défaut).

### 🔁 Fonctionnement détaillé

1. Génération d’un ID unique pour chaque worker
2. Initialisation de la base et nettoyage des verrous
3. Récupération de la liste des fichiers SFTP
4. Réservation et traitement parallèle
5. Transfert : SFTP ➝ Local ➝ MinIO
6. Mise à jour du statut en base

### 🧭 Suivi & Logs
- Logs par worker dans `/opt/airflow/logs/`
- Statistiques détaillées sur chaque fichier
- Suivi des erreurs et performances

---

## 🗃️ Base de données : `file_transfer_log`

| Colonne             | Type           | Description                          |
|---------------------|----------------|--------------------------------------|
| id                  | SERIAL         | Clé primaire                         |
| filename            | VARCHAR(255)   | Nom du fichier (unique)             |
| worker_id           | VARCHAR(100)   | ID du worker                         |
| transfer_start_time | TIMESTAMP      | Début du transfert                   |
| transfer_end_time   | TIMESTAMP      | Fin du transfert                     |
| file_size_mb        | DECIMAL(10,2)  | Taille du fichier                    |
| download_duration   | DECIMAL(10,2)  | Durée de téléchargement              |
| upload_duration     | DECIMAL(10,2)  | Durée d’upload                       |
| total_duration      | DECIMAL(10,2)  | Durée totale                         |
| status              | VARCHAR(20)    | IN_PROGRESS, COMPLETED, FAILED       |
| error_message       | TEXT           | Message d’erreur s’il y a lieu       |
| created_at          | TIMESTAMP      | Date de création                     |
| updated_at          | TIMESTAMP      | Date de mise à jour                  |

---

## 🛠️ Maintenance

- **Nettoyage des verrous** : automatique au démarrage de chaque worker
- **Statistiques** : fichiers traités, taille, vitesse, erreurs
- **Logs d’erreur** : contexte complet + trace

---

## 🌱 Évolutions futures

- 🔁 Retry automatique en cas d’échec
- 📦 Compression durant les transferts
- 🔗 Support multi-serveurs source
- 📊 Interface web de monitoring
- 🚨 Alertes automatiques en cas d’erreurs