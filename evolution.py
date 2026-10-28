import json
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

REGISTRY = "http://localhost:8081"
SUBJECT = "sensors.readings.avro-value"

registry = SchemaRegistryClient({"url": REGISTRY})

print("Règle de compatibilité :", registry.get_compatibility())

# On récupère le schéma actuel (version 1)
actuel = registry.get_latest_version(SUBJECT)
print("Version actuelle :", actuel.version)
schema_v1 = json.loads(actuel.schema.schema_str)

# --- Essai 1 : nouveau champ SANS valeur par défaut ---
v2_mauvais = json.loads(json.dumps(schema_v1))
v2_mauvais["fields"].append({"name": "vibration", "type": "double"})

ok = registry.test_compatibility(SUBJECT, Schema(json.dumps(v2_mauvais), "AVRO"))
print("Essai 1 (sans défaut) compatible ?", ok)

# --- Essai 2 : nouveau champ AVEC valeur par défaut ---
v2_bon = json.loads(json.dumps(schema_v1))
v2_bon["fields"].append({"name": "vibration", "type": ["null", "double"], "default": None})

ok = registry.test_compatibility(SUBJECT, Schema(json.dumps(v2_bon), "AVRO"))
print("Essai 2 (avec défaut) compatible ?", ok)

if ok:
    schema_id = registry.register_schema(SUBJECT, Schema(json.dumps(v2_bon), "AVRO"))
    print("Version 2 enregistrée, id :", schema_id)
