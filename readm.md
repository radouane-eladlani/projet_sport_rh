# 🚴‍♂️ Projet Sport & RH — Architecture Data Lakehouse de Bout en Bout

## 📖 Présentation

Ce projet met en œuvre une **architecture Data Lakehouse complète** simulant un environnement de production. Il couvre l'ensemble du cycle de vie de la donnée, depuis sa génération jusqu'à sa restitution dans un tableau de bord décisionnel.

L'architecture combine :

- 📥 **Ingestion de données** via API REST
- ⚡ **Streaming temps réel** avec CDC (Change Data Capture)
- 🏗️ **Architecture Medallion** (Bronze → Silver → Gold)
- ✅ **Contrôles de qualité** des données
- 📊 **Visualisation** des indicateurs métier dans Tableau Public

> L'objectif est de reproduire un pipeline Data Engineering moderne utilisant les principales briques d'un environnement Big Data.

---

## 🏗️ Architecture technique

Le pipeline est organisé en **6 grandes étapes**.

### 0. Référentiel RH

Préparation des données de référence de l'entreprise :

- Lecture des fichiers Excel RH
- Nettoyage des données
- Fusion des référentiels
- Calcul des distances domicile ↔ entreprise

➡️ **Résultat** : production d'un référentiel propre utilisé dans les traitements suivants.

### 1. Ingestion live

Simulation d'activités sportives réalisées par les collaborateurs :

- Génération automatique des activités
- Envoi via une API **FastAPI**
- Enregistrement dans **PostgreSQL**

### 2. CDC & Streaming

Chaque insertion dans PostgreSQL est détectée automatiquement grâce à **Debezium**.

Debezium lit le WAL PostgreSQL puis publie les événements dans **Redpanda (Kafka)**, permettant une diffusion des données en temps réel.

### 3. Consommation temps réel

Deux consommateurs exploitent simultanément les événements :

| Consommateur | Rôle |
|---|---|
| 🔔 **Slack Bot** | Écoute le topic Redpanda, génère un message personnalisé et envoie instantanément une notification Slack |
| ⚡ **Spark Structured Streaming** | Lit le même flux Kafka et écrit les données brutes dans la couche Delta Lake **Bronze** |

### 4. Traitement batch (architecture Medallion)

**🥈 Couche Silver**

Spark réalise :
- Nettoyage
- Enrichissement
- Jointure avec le référentiel RH

Les données deviennent exploitables.

**🥇 Couche Gold**

Spark calcule les indicateurs métier :
- Nombre d'activités
- Distance parcourue
- Statistiques RH

Les résultats sont stockés dans **Delta Lake Gold**.

### 5. Restitution

Les KPI sont exportés automatiquement au format **CSV** (via `coalesce(1)`).

Ces fichiers alimentent un **tableau de bord interactif** réalisé avec **Tableau Public**.

---

## 🚀 Guide d'exécution

### 1. Démarrer l'infrastructure Docker

```bash
docker compose up -d
```

Cette commande démarre :
- PostgreSQL
- Debezium
- Redpanda
- Kafka Connect

### 2. Activer l'environnement Python

```bash
source sport_rh_env/bin/activate
pip install -r requirements.txt
```

### 3. Construire le référentiel RH

```bash
python scripts/01_lire_donnees.py
python scripts/02_nettoyer_donnees.py
python scripts/03_fusion_data_valide_distance.py
```

### 4. Initialiser PostgreSQL

```bash
python scripts/05a_init_postgres.py
```

### 5. Initialiser Debezium

⚠️ Pensez à remplir les champs de configuration (host, port, user, password, dbname, topic.prefix) avant d'exécuter la requête.

```bash
curl -i -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  localhost:8083/connectors/ \
  -d '{
    "name": "sport-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "tasks.max": "1",
      "database.hostname": "",
      "database.port": "",
      "database.user": "",
      "database.password": "",
      "database.dbname": "",
      "topic.prefix": "",
      "table.include.list": "public.activites_sportives",
      "plugin.name": "pgoutput"
    }
  }'
```

### 6. Lancer l'ingestion des données

**Démarrer l'API**
```bash
python scripts/05b_API_ingestion.py
```

**Générer des activités sportives**
```bash
python scripts/04_generer_activites.py
```

Les données sont désormais insérées dans PostgreSQL.

### 7. Lancer les traitements temps réel

Ouvrir deux terminaux.

**Terminal 1 — Notifications Slack**
```bash
python scripts/06_consommer_slack.py
```

**Terminal 2 — Streaming Spark**
```bash
python scripts/07_ingestion_streaming.py
```

À cette étape :
- Les notifications apparaissent sur Slack
- Les données sont enregistrées dans la couche Bronze

**Contrôle qualité :**
```bash
python scripts/check_bronze.py
```

### 8. Construire la couche Silver

```bash
python scripts/08_creer_silver.py
```

**Contrôle qualité :**
```bash
python scripts/check_silver.py
```

### 9. Construire la couche Gold

```bash
python scripts/09_creer_gold.py
```

**Contrôle qualité :**
```bash
python scripts/check_gold.py
```

Les KPI sont ensuite exportés automatiquement en CSV afin d'alimenter le tableau de bord Tableau Public.

---

## 📊 Pipeline de données

```
Excel RH
   │
   ▼
Référentiel Entreprise
   │
   ▼
Générateur d'activités
   │
   ▼
FastAPI
   │
   ▼
PostgreSQL
   │
   ▼
Debezium (CDC)
   │
   ▼
Redpanda (Kafka)
   │
   ├──────────────► Slack Bot
   │
   ▼
Spark Streaming
   │
   ▼
Delta Bronze
   │
   ▼
Spark Batch
   │
   ▼
Delta Silver
   │
   ▼
Spark Batch
   │
   ▼
Delta Gold
   │
   ▼
Export CSV
   │
   ▼
Tableau Public
```

---

## 📁 Structure du projet

```
PROJET_SPORT_RH/
│
├── data/
│   ├── lake/
│   │   ├── bronze/
│   │   ├── checkpoints/
│   │   ├── silver/
│   │   └── gold/
│   ├── donnees_fusionnees.csv
│   ├── Données+RH.xlsx
│   └── Données+Sportive.xlsx
│
├── scripts/
│   ├── 01_lire_donnees.py
│   ├── 02_nettoyer_donnees.py
│   ├── 03_fusion_data_valide_distance.py
│   ├── 04_generer_activites.py
│   ├── 05a_init_postgres.py
│   ├── 05b_API_ingestion.py
│   ├── 06_consommer_slack.py
│   ├── 07_ingestion_streaming.py
│   ├── 08_creer_silver.py
│   ├── 09_creer_gold.py
│   ├── check_bronze.py
│   ├── check_silver.py
│   └── check_gold.py
│
├── yaml/                  # Workflows Kestra
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

---

## 🛠️ Technologies utilisées

- Python
- Apache Spark / Spark Structured Streaming
- Delta Lake
- PostgreSQL
- Debezium
- Redpanda (Kafka API)
- FastAPI
- Docker
- Slack Webhook
- Kestra
- Tableau Public