import json
import time
from datetime import datetime, timedelta

import pandas as pd
from confluent_kafka import Producer

TOPIC = "sensors.readings"
BOOTSTRAP = "localhost:9092"

colonnes = (
    ["unit", "cycle", "setting_1", "setting_2", "setting_3"]
    + [f"s{i}" for i in range(1, 22)]
)

df = pd.read_csv(
    "data/train_FD001.txt",
    sep=r"\s+",
    header=None,
    names=colonnes
)

moteur = df[df["unit"] == 1]

producer = Producer({"bootstrap.servers": BOOTSTRAP})

def rapport(err, msg):
    if err:
        print("ERREUR :", err)
    else:
        print(f"OK -> partition {msg.partition()}, offset {msg.offset()}")

date_depart = datetime(2024, 1, 1)

for _, ligne in moteur.iterrows():
    message = ligne.to_dict()
    message["equipment_id"] = f"EQ-{int(ligne['unit']):04d}"
    message["event_time"] = (
        date_depart + timedelta(days=int(ligne["cycle"]))
    ).isoformat()

    del message["unit"]

    producer.produce(
        TOPIC,
        key=message["equipment_id"],
        value=json.dumps(message),
        callback=rapport,
    )

    producer.poll(0)
    time.sleep(0.05)

producer.flush()
print("Terminé")
