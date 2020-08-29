FROM confluentinc/cp-kafka-connect-base:5.5.1
RUN  confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:5.5.1
RUN  confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:1.2.0