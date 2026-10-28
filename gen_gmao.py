import os 
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

random.seed(42)   # mêmes données à chaque exécution (reproductible)

DB = dict(host="localhost", port=5433, user="dataengineer",
            password=os.environ["GMAO_DB_PASSWORD"], dbname="gmao")
DEPART = datetime(2024, 1, 1, 8, 0, tzinfo=timezone.utc)

# Dernier cycle de chaque moteur = date de la panne
df = pd.read_csv("data/train_FD001.txt", sep=r"\s+", header=None,
                 usecols=[0, 1], names=["unit", "cycle"])
dernier_cycle = df.groupby("unit")["cycle"].max()

ATELIERS = {"MIXER": "Atelier A", "EXTRUDER": "Atelier B", "CURING_PRESS": "Atelier C"}

CAUSES = {
    "vibration": [
        "Vibrations anormales sur le rotor, roulement remplacé.",
        "vibrations fortes cote moteur, roulement HS, remplacé + graissage",
        "Bruit suspect et vibrations, palier usé, changement palier.",
    ],
    "surchauffe": [
        "Surchauffe du fourreau, sonde de température défectueuse remplacée.",
        "temp trop haute, ventilateur bloqué, nettoyé et remis en route",
        "Arrêt sur alarme température, circuit de refroidissement encrassé.",
    ],
    "usure": [
        "Usure de la vis constatée, pièce remplacée.",
        "joint use + fuite d'huile, remplacement joint",
        "Courroie très usée, remplacée. Prévoir contrôle des autres.",
    ],
    "electrique": [
        "Défaut variateur, carte électronique remplacée.",
        "disjoncteur qui saute, câble moteur abimé, remplacé",
        "Perte de signal capteur de pression, connecteur oxydé nettoyé.",
    ],
    "fuite": [
        "Fuite vapeur sur la presse, vanne changée.",
        "fuite hydraulique au niveau du vérin, flexible remplacé",
    ],
}
PREVENTIF = ["Graissage et contrôle périodique, RAS.",
             "Maintenance préventive réalisée selon gamme, RAS.",
             "controle visuel + serrage, rien à signaler"]

equipements, ordres = [], []
for unit, fin in dernier_cycle.items():
    eq_id = f"EQ-{int(unit):04d}"
    famille = random.choice(list(ATELIERS))
    equipements.append((eq_id, famille, ATELIERS[famille],
                        random.choice([1, 2, 2, 3, 3])))

    # Ordre correctif : le jour de la panne
    cause = random.choice(list(CAUSES))
    ordres.append((eq_id, "CORRECTIVE",
                   DEPART + timedelta(days=int(fin)),
                   round(random.uniform(4, 48), 2),
                   random.choice(CAUSES[cause])))

    # 1 à 3 ordres préventifs pendant la vie de la machine
    for _ in range(random.randint(1, 3)):
        ordres.append((eq_id, "PREVENTIVE",
                       DEPART + timedelta(days=random.randint(1, int(fin) - 1)),
                       round(random.uniform(1, 4), 2),
                       random.choice(PREVENTIF)))

conn = psycopg2.connect(**DB)
with conn, conn.cursor() as cur:
    # On repart de zéro (ces DELETE seront aussi vus par Debezium)
    cur.execute("DELETE FROM work_orders")
    cur.execute("DELETE FROM equipment")
    execute_values(cur,
        "INSERT INTO equipment (equipment_id, family, workshop, criticality) VALUES %s",
        equipements)
    execute_values(cur,
        "INSERT INTO work_orders (equipment_id, wo_type, opened_at, downtime_hours, technician_comment) VALUES %s",
        ordres)
conn.close()
print(f"{len(equipements)} équipements, {len(ordres)} ordres de travail insérés")
