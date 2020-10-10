FROM confluentinc/cp-kafka-connect-base:5.5.1
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:1.2.2
RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:1.3.0
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.0.0