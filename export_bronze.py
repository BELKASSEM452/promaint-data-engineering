import json
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

BOOTSTRAP = "localhost:9092"
REGISTRY = "http://localhost:8081"

SORTIE = Path("data/bronze")
SORTIE.mkdir(parents=True, exist_ok=True)

deser = AvroDeserializer(
    SchemaRegistryClient({"url": REGISTRY})
)


def lire(topic, group):
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    c.subscribe([topic])

    lignes = []
    vides = 0

    while vides < 2:
        msg = c.poll(5.0)

        if msg is None:
            vides += 1
            continue

        if msg.error():
            print("Erreur Kafka :", msg.error())
            continue

        vides = 0

        if msg.value() is None:
            continue

        valeur = deser(
            msg.value(),
            SerializationContext(topic, MessageField.VALUE)
        )

        lignes.append((
            valeur,
            msg.partition(),
            msg.offset(),
            msg.timestamp()[1]
        ))

    c.close()
    return lignes


# 1. Exporter les capteurs
rows = []

for valeur, part, off, ts in lire("sensors.readings.avro", "export-capteurs"):
    row = dict(valeur)
    row["_kafka_partition"] = part
    row["_kafka_offset"] = off
    row["_kafka_ts"] = ts
    rows.append(row)

df_cap = pd.DataFrame(rows)

df_cap.to_parquet(
    SORTIE / "sensors_readings.parquet",
    index=False
)

print("Capteurs :", len(df_cap), "lignes")


# 2. Exporter les ordres GMAO
rows = []

for valeur, part, off, ts in lire(
    "gmao_avro.public.work_orders",
    "export-gmao-avro"
):
    rows.append({
        "op": valeur["op"],
        "ts_ms": valeur["ts_ms"],
        "before_json": json.dumps(valeur["before"], default=str),
        "after_json": json.dumps(valeur["after"], default=str),
        "_kafka_partition": part,
        "_kafka_offset": off,
        "_kafka_ts": ts
    })

df_gmao = pd.DataFrame(rows)

df_gmao.to_parquet(
    SORTIE / "gmao_work_orders.parquet",
    index=False
)

print("GMAO :", len(df_gmao), "lignes")
