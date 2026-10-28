FROM quay.io/debezium/connect:3.3 AS debezium

FROM confluentinc/cp-kafka-connect:8.2.0

COPY --from=debezium /kafka/connect/debezium-connector-postgres /usr/share/confluent-hub-components/debezium-connector-postgres
