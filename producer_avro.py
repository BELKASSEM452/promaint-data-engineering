import json
from datetime import datetime, timedelta

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

TOPIC = "sensors.readings.avro"
BOOTSTRAP = "localhost:9092"
REGISTRY = "http://localhost:8081"   # adapte si besoin

CAPTEURS = [f"s{i}" for i in range(1, 22)]
REGLAGES = ["setting_1", "setting_2", "setting_3"]

# Le contrat : la forme exacte d'un message
schema = {
    "type": "record",
    "name": "SensorReading",
    "namespace": "promaint",
    "fields": (
        [
            {"name": "equipment_id", "type": "string"},
            {"name": "event_time", "type": "string"},
            {"name": "cycle", "type": "int"},
        ]
        + [{"name": n, "type": "double"} for n in REGLAGES + CAPTEURS]
    ),
}

registry = SchemaRegistryClient({"url": REGISTRY})
serializer = AvroSerializer(registry, json.dumps(schema))
producer = Producer({"bootstrap.servers": BOOTSTRAP})

# Les 100 moteurs (20 631 mesures)
df = pd.read_csv(
    "data/train_FD001.txt",
    sep=r"\s+",
    header=None,
    names=["unit", "cycle"] + REGLAGES + CAPTEURS,
)

erreurs = 0


def rapport(err, msg):
    """Appelée par Kafka pour chaque message : confirme la livraison ou signale l'échec."""
    global erreurs
    if err:
        erreurs += 1
        print("ERREUR :", err)


date_depart = datetime(2024, 1, 1)
envoyes = 0

for _, ligne in df.iterrows():
    msg = {
        "equipment_id": f"EQ-{int(ligne['unit']):04d}",
        "event_time": (date_depart + timedelta(days=int(ligne["cycle"]))).isoformat(),
        "cycle": int(ligne["cycle"]),
    }
    for n in REGLAGES + CAPTEURS:
        msg[n] = float(ligne[n])

    producer.produce(
        TOPIC,
        key=msg["equipment_id"],
        value=serializer(msg, SerializationContext(TOPIC, MessageField.VALUE)),
        on_delivery=rapport,
    )
    producer.poll(0)
    envoyes += 1
    if envoyes % 5000 == 0:
        print(f"{envoyes} messages envoyés...")

producer.flush()
print(f"Terminé : {envoyes} envoyés, {erreurs} erreurs")