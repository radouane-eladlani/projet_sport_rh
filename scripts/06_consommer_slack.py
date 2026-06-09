# ==============================================================================
# SCRIPT 06 : CONSOMMATEUR REDPANDA & NOTIFICATION SLACK (TEMPS RÉEL)
# ==============================================================================

import json
import os
import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer

# On charge le fichier .env qui est dans le dossier parent
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
REDPANDA_BROKER = "localhost:19092"
TOPIC_NAME = "dbserver1.public.activites_sportives"

print("=" * 60)
print("             BOT EMULATION SPORTIVE EN MARCHE            ")
print("=" * 60)

if not SLACK_WEBHOOK_URL:
    print("❌ ERREUR : L'URL Slack n'est pas détectée dans le fichier .env.")
    exit(1)

def envoyer_a_slack(texte):
    headers = {"Content-Type": "application/json"}
    payload = {"text": texte}
    try:
        response = requests.post(SLACK_WEBHOOK_URL, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            print("👉 Notification envoyée sur Slack avec succès !")
        else:
            print(f"❌ Échec Slack (Code {response.status_code})")
    except Exception as e:
        print(f"❌ Erreur de connexion Slack : {e}")

try:
    print(f"Connexion à Redpanda ({REDPANDA_BROKER})...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[REDPANDA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x is not None else None
    )
    print(f"Écoute active sur le topic : '{TOPIC_NAME}'...")
    print("En attente d'une nouvelle activité... (Laisse ce terminal ouvert)\n")
except Exception as e:
    print(f"❌ Erreur de connexion : {e}")
    exit(1)

# Limiter les notifications à 5 
compteur_notifications = 0
LIMITE_NOTIFICATIONS = 15

print(f"🚀 Sécurité : Seuls les {LIMITE_NOTIFICATIONS} premiers messages iront sur Slack.")
print("-" * 60)

for message in consumer:
    # 1. Sécurité : On ignore les messages vides (tombstones)
    if message.value is None:
        continue
        
    evenement = message.value
    
    # 2. On vérifie si "payload" existe dans le JSON
    payload_debezium = evenement.get("payload", {}) if evenement else {}
    
    # Validation de l'opération 'c' (Create) ou 'u' (Update)
    operation = payload_debezium.get("op")
    
    if operation in ["c", "u"]:
        donnees = payload_debezium.get("after", {})
        
        # 3. Récupération du commentaire
        commentaire = donnees.get("Commentaire")
        
        if commentaire:
            if compteur_notifications < LIMITE_NOTIFICATIONS:
                print(f"🔥 Nouvelle ligne capturée en BDD !")
                print(f"   Message : \"{commentaire}\"")
                envoyer_a_slack(commentaire)
                compteur_notifications += 1
                print(f"   [Sécurité] {compteur_notifications}/{LIMITE_NOTIFICATIONS} notifications envoyées.\n")
                print("-" * 50)
            else:
                print(f"🤫 Ligne capturée (Masquée sur Slack pour éviter le spam) : {commentaire[:75]}...")