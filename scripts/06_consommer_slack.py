# ==============================================================================
# 06 - CONSOMMATEUR KAFKA → SLACK
# ==============================================================================
# OBJECTIF DU SCRIPT :
# Ce script écoute en temps réel les événements produits dans Kafka (Redpanda)
# par Debezium (insert PostgreSQL).
#
# À chaque nouvelle activité sportive :
# - on récupère l'événement
# - on extrait les données utiles
# - on génère un message personnalisé
# - on envoie le message sur Slack via webhook
# ==============================================================================

import json
import os
import requests
import random
from kafka import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime

# ==============================================================================
# 1. CHARGEMENT DES VARIABLES D'ENVIRONNEMENT
# ==============================================================================

load_dotenv()

SLACK = os.getenv("SLACK_WEBHOOK_URL")

# ==============================================================================
# 2. CONNEXION À KAFKA (REDPANDA)
# ==============================================================================
# On écoute le topic généré automatiquement par Debezium
# Chaque message correspond à une insertion dans PostgreSQL

consumer = KafkaConsumer(
    "dbserver1.public.activites_sportives",
    bootstrap_servers="localhost:19092",
    auto_offset_reset="earliest",   # lit depuis le début du topic
    enable_auto_commit=True,        # mémorise la position de lecture
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None
)

print("Slack bot actif et à l'écoute de Redpanda...")

# ==============================================================================
# 3. TEMPLATES DE MESSAGES SLACK
# ==============================================================================
# Chaque sport possède plusieurs messages possibles
# On choisit un message aléatoire pour rendre Slack plus naturel

TEMPLATES = {
    "RUNNING": [
        "🏃‍♂️ Incroyable {name} ! Tu viens de courir {distance:.1f} km en {duration} min ! Quelle énergie ! 🔥🏅",
        "🔥 Bravo {name} ! {distance:.1f} km de running validés ! 💪"
    ],

    "RUNING": [
        "🏃‍♂️ Bravo {name} ! {distance:.1f} km de course validés ! 🔥"
    ],

    "JUDO": [
        "🥋 Magnifique {name} ! Séance de judo intense ! 💪",
        "🥋 Bravo {name} ! Super progression en judo 👊"
    ],

    "NATATION": [
        "🏊 Super {name} ! Tu viens de nager {distance:.1f} km en {duration} min ! 🔥🏅",
        "🌊 Excellent {name} ! Belle séance de natation 💪"
    ],

    "FOOTBALL": [
        "⚽ Bravo {name} ! Session football intense 🔥",
        "⚽ Excellent {name} ! Très belle énergie sur le terrain 💪"
    ],

    "BASKETBALL": [
        "🏀 Bravo {name} ! Match intense 🔥",
        "🏀 Super {name} ! Très grosse énergie 💪"
    ],

    "TENNIS": [
        "🎾 Excellent {name} ! Belle session de tennis 🔥",
        "🎾 Bravo {name} ! Très bon match 💪"
    ],

    "TENNIS DE TABLE": [
        "🏓 Bravo {name} ! Session rapide et technique 🔥"
    ],

    "BADMINTON": [
        "🏸 Super {name} ! Match dynamique 🔥"
    ],

    "RUGBY": [
        "🏉 Bravo {name} ! Séance intense 💪🔥"
    ],

    "RANDONNÉE": [
        "🌄 Magnifique {name} ! {distance:.1f} km de randonnée 🌿",
        "🥾 Bravo {name} ! Super randonnée terminée 🌄"
    ],

    "ESCALADE": [
        "🧗 Bravo {name} ! Très belle session d’escalade 🔥"
    ],

    "BOXE": [
        "🥊 Bravo {name} ! Session intense 💪🔥"
    ],

    "ÉQUITATION": [
        "🐎 Magnifique {name} ! Belle séance 🌿"
    ],

    "VOILE": [
        "⛵ Super {name} ! Belle navigation 🔥"
    ],

    "TRIATHLON": [
        "🔥 Incroyable {name} ! Triathlon terminé 💪🏅"
    ],

    # Message par défaut si le sport n'est pas reconnu
    "DEFAULT": [
        "🔥 Bravo {name} ! Activité {sport} réalisée avec succès 💪"
    ]
}

# ==============================================================================
# 4. FONCTION DE PARSING DES DATES
# ==============================================================================
# Convertit les dates provenant de Debezium en objets datetime Python 
# pour éviter les erreurs dans le traitement streaming

def parse_date(value):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Format de date non reconnu : {value}")

# ==============================================================================
# 5. FONCTION D'ENVOI VERS SLACK
# ==============================================================================
# Envoie un message via webhook Slack

def send(msg):
    try:
        requests.post(SLACK, json={"text": msg})
    except Exception as e:
        print(f"Erreur d'envoi Slack : {e}")
        duration_min = 0

# ==============================================================================
# 6. BOUCLE PRINCIPALE KAFKA
# ==============================================================================
# Le script tourne en continu et attend les nouveaux événements

for msg in consumer:

    # Ignore les messages vides
    if not msg.value:
        continue

# Debezium encapsule chaque événement dans un objet "payload" contenant before/after/op.
# On récupère "payload" pour accéder aux données CDC (Change Data Capture) du message Kafka.
# On extrait "after" car il contient la version finale de la ligne après insertion/modification en base.    
    payload = msg.value.get("payload", {})
    after = payload.get("after")

    # Ignore les suppressions ou événements incomplets
    if not after:
        continue

    # ======================================================================
    # 7. EXTRACTION DES DONNÉES
    # ======================================================================

    # Sport pratiqué (normalisation)
    sport = (after.get("Type") or "UNKNOWN").upper().strip()

    # Distance en mètres
    distance_m = after.get("Distance") or 0

    # ID salarié
    user_id = after.get("ID salarié") or "inconnu"

    # Commentaire contenant prénom + nom
    full_comment = after.get("Commentaire") or ""

    # Extraction du nom propre
    if " - " in full_comment:
        name = full_comment.split(" - ")[0]
    else:
        name = full_comment if full_comment else f"salarié {user_id}"

    # ======================================================================
    # 8. TRAITEMENTS MÉTIER
    # ======================================================================

    # Conversion mètres -> kilomètres
    try:
        distance_km = float(distance_m) / 1000
    except:
        distance_km = 0.0

    # Calcul de la durée en minutes
    duration_min = 0
    try:
        start_str = after.get("Date de début de l'activité")
        end_str = after.get("Date de fin de l'activité")

        if start_str and end_str:
            start = parse_date(start_str)
            end = parse_date(end_str)
            duration_min = int((end - start).total_seconds() // 60)

    except:
        duration_min = 0

    # ======================================================================
    # 9. GÉNÉRATION DU MESSAGE
    # ======================================================================

    # Sélection du template selon le sport
    template_list = TEMPLATES.get(sport, TEMPLATES["DEFAULT"])

    # Choix aléatoire du message
    text_template = random.choice(template_list)

    # Construction du message final
    try:
        message = text_template.format(
            name=name,
            sport=sport,
            distance=distance_km,
            duration=duration_min
        )
    except:
        message = f"🔥 Bravo {name} ! Activité {sport} réalisée 💪"

    # ======================================================================
    # 10. ENVOI SLACK
    # ======================================================================

    send(message)

    print(f"Message envoyé pour {name} ({sport})")