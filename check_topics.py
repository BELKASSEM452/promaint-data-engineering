from collections import Counter

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

BOOTSTRAP = "localhost:9092"
REGISTRY = "http://localhost:8081"

deser = AvroDeserializer(SchemaRegistryClient({"url": REGISTRY}))


def lire_topic(topic, group):
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # on relit tout à chaque exécution
    })
    c.subscribe([topic])
    messages, illisibles, vides = [], 0, 0
    while vides < 2:                   # on s'arrête après ~10 s sans message
        msg = c.poll(5.0)
        if msg is None:
            vides += 1
            continue
        vides = 0
        if msg.error():
            print("Erreur Kafka :", msg.error())
            continue
        if msg.value() is None:        # tombstone Debezium (suite d'un DELETE)
            continue
        try:
            messages.append(deser(msg.value(), SerializationContext(topic, MessageField.VALUE)))
        except Exception as e:
            illisibles += 1
            if illisibles == 1:
                print(f"[{topic}] message illisible en Avro :", str(e)[:150])
    c.close()
    return messages, illisibles


print("=== CAPTEURS ===")
capteurs, ill = lire_topic("sensors.readings.avro", "check-capteurs")
uniques = {(m["equipment_id"], m["cycle"]) for m in capteurs}
print("Nombre de messages :", len(capteurs))

for m in capteurs[:3]:
    print("Message :", m)
ids_capteurs = {e for e, _ in uniques}
print("Messages lus            :", len(capteurs), "| illisibles :", ill)
print("Mesures uniques         :", len(uniques), "(attendu : 20631)")
print("Doublons                :", len(capteurs) - len(uniques))
print("Équipements distincts   :", len(ids_capteurs), "(attendu : 100)")

print("\n=== GMAO ===")
gmao, ill = lire_topic("gmao_avro.public.work_orders", "check-gmao-avro")
print("Messages lus            :", len(gmao), "| illisibles :", ill)
if gmao:
    print("Opérations (op)         :", dict(Counter(m["op"] for m in gmao)))
    ordres = {m["after"]["wo_id"] for m in gmao if m["after"]}
    print("Ordres distincts        :", len(ordres))
    ids_gmao = {m["after"]["equipment_id"] for m in gmao if m["after"]}
    print("Équipements GMAO sans capteurs :", sorted(ids_gmao - ids_capteurs) or "aucun")
