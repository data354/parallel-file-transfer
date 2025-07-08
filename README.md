# Système de Transfert Parallèle SFTP vers MinIO

Ce projet implémente un système de transfert parallèle de fichiers depuis un serveur SFTP vers un stockage MinIO, utilisant Apache Airflow pour l'orchestration.

## Architecture

Le projet est divisé en plusieurs modules pour une meilleure maintenabilité :

### Structure des fichiers

```
├── dag_config.py                 # Configuration et constantes
├── database_manager.py       # Gestionnaire de base de données PostgreSQL
├── connection_handler.py     # Gestionnaire de connexions persistantes
├── dag.py              # DAG Airflow principal
└── README.md               # Documentation
```

## Fonctionnalités

### Transfert parallèle
- Utilise un pool de threads pour traiter plusieurs fichiers simultanément
- Connexions persistantes pour optimiser les performances
- Gestion automatique des ressources

### Coordination multi-workers
- Système de verrouillage distribué via PostgreSQL
- Évite les transferts dupliqués entre workers
- Nettoyage automatique des verrous obsolètes

### Surveillance et logging
- Logging détaillé par worker et thread
- Statistiques de performance en temps réel
- Diagnostic des workers actifs

### Robustesse
- Gestion complète des erreurs
- Nettoyage automatique des ressources
- Récupération après panne

## Configuration

### Connexions Airflow requises

1. **ssh_connection** : Connexion SSH vers le serveur source
   ```json
   {
    'Connection Id' : 'ssh_connection',
    'Connection Type' : 'SSH'
     "host": "server.host",
     "port": 22,
     "login": "username",
     "extra": {
       "key_file": "/path/to/private/key", ( "key_file": "/home/ubuntu/.ssh/id_rsa")
     }
   }
   ```

2. **minio_connect** : Connexion MinIO
   ```json
   {
   ' Connection Id' : 'minio_connect',
   'Connection Type' : 'Amazon Web Services',
   'AWS Access Key ID' : 'minio-key',
    'AWS Secret Access Key' :' minio-secret',
    "extra": {
       "endpoint_url": "minio.host:9000"
     }
   }
   ```

3. **transfer_db** : Connexion PostgreSQL
   ```json
   {
    ' Connection Id' : 'transfer_bd',
    'Connection Type' : 'postgres',
     "host": "postgres.host",
     "port": 5432,
     "login": "username",
     "password": "password",
     "database": "database_name"
   }
   ```

### Variables configurables

Modifiez les constantes dans `config.py` :

- `MAX_WORKERS` : Nombre de threads par worker (défaut: 4)
- `LOCAL_DIR` : Répertoire temporaire local
- `BUCKET_NAME` : Nom du bucket MinIO de destination
- `MAX_STALE_LOCK_MINUTES` : Durée avant nettoyage des verrous (défaut: 30min)



## Utilisation

Le DAG se déclenche automatiquement selon la planification configurée (`@hourly` par défaut).

### Fonctionnement

1. Chaque worker génère un ID unique
2. Initialisation de la base de données et nettoyage des verrous
3. Récupération de la liste des fichiers à traiter
4. Traitement parallèle avec réservation de fichiers
5. Transfert SFTP → Local → MinIO
6. Mise à jour du statut en base de données

### Monitoring

Les logs sont stockés dans `/opt/airflow/logs/` avec un fichier par worker :
- Informations détaillées sur chaque transfert
- Statistiques de performance
- Diagnostic des erreurs

## Base de données

### Table `file_transfer_log`

| Colonne | Type | Description |
|---------|------|-------------|
| id | SERIAL | Clé primaire |
| filename | VARCHAR(255) | Nom du fichier (unique) |
| worker_id | VARCHAR(100) | ID du worker |
| transfer_start_time | TIMESTAMP | Début du transfert |
| transfer_end_time | TIMESTAMP | Fin du transfert |
| file_size_mb | DECIMAL(10,2) | Taille en MB |
| download_duration | DECIMAL(10,2) | Durée téléchargement |
| upload_duration | DECIMAL(10,2) | Durée upload |
| total_duration | DECIMAL(10,2) | Durée totale |
| status | VARCHAR(20) | Statut (IN_PROGRESS, COMPLETED, FAILED) |
| error_message | TEXT | Message d'erreur |
| created_at | TIMESTAMP | Date création |
| updated_at | TIMESTAMP | Date mise à jour |

## Optimisations

- **Connexions persistantes** : Réutilisation des connexions SSH/SFTP et MinIO
- **Traitement parallèle** : Threads multiples pour maximiser le débit
- **Gestion mémoire** : Nettoyage automatique des fichiers temporaires
- **Coordination distribuée** : Évite les conflits entre workers multiples

## Maintenance

### Nettoyage des verrous obsolètes
Les verrous de plus de 30 minutes sont automatiquement nettoyés au démarrage de chaque worker.

### Monitoring des performances
Les statistiques incluent :
- Nombre de fichiers traités
- Taille totale transférée
- Vitesse moyenne de transfert
- Durée par phase (téléchargement/upload)

### Diagnostic des erreurs
Chaque erreur est loggée avec :
- Contexte complet (worker, thread, fichier)
- Message d'erreur détaillé
- Statut mis à jour en base

## Évolutions possibles

- Ajout de retry automatique pour les échecs
- Compression des fichiers pendant le transfert
- Support de multiples serveurs sources
- Interface web de monitoring
- Alertes en cas d'erreurs répétées