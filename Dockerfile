FROM confluentinc/cp-kafka-connect-base:5.5.1
RUN  confluent-hub install --no-prompt debezium/debezium-connector-postgresql:1.2.1
RUN  confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:1.2.0