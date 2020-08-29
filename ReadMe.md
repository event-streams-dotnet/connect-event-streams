# Kafka Connect with Event Streams

Use Kafka Connect to transfer data in real-time between a source and sink with an event stream processor that can perform data transformations.

> **Note**: See this blog post explaining the design of the [event stream processing framework](https://blog.tonysneed.com/2020/06/25/event-stream-processing-micro-framework-apache-kafka/) used here.

### Prerequisites

1. Install [Docker Desktop](https://docs.docker.com/desktop/).
   - You will need at least 8 GB of available memory.
2. Open a terminal at the project root and run `docker-compose up --build -d`.
   - To check the running containers run `docker-compose ps`.
   - To bring down the containers run `docker-compose down`.
3. Open a browser to http://localhost:9021/.
   - Verify the cluster is healthy. (This may take a few minutes.)

## Source Connector

1. Open **pgadmin** and connect to **postgres**.
   - Navigate to http://localhost:5050
   - Username: pgadmin4@pgadmin.org
   - Password: admin
   - Click **Add New Server**
      - Name: postgres
      - Connection **host name**: postgres
      - Connection **username**: postgres
      - Connection **password**: mypassword

2. Create `person` table.
   - Select **source-database**.
   - Right-click and select Query Tool.
   - Execute script to create table.
   ```sql
   CREATE TABLE public.person
   (
      person_id serial NOT NULL,
      first_name text NOT NULL,
      last_name text NOT NULL,
      favorite_color text NOT NULL,
      age integer NOT NULL,
      row_version timestamp with time zone NOT NULL,
      CONSTRAINT person_pkey PRIMARY KEY (person_id)
   )
   ```

3. Register JDBC source connector.
   - JDBC connector [features](https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/index.html#features).
   - JDBC connector [configuration](https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html).

   - *Option 1*: Upload the connector config `json` file using the **Control Center** user interface.
      - The **register-postgres-source.json** file is located in the **Connectors** folder.
   - *Option 2*: Run the following `curl` command to upload the config `json` file.

    ```bash
    curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @Connectors/register-postgres-source.json
    ```

4. Add a row to the Postgres `public.person` database.
   - Open **pgAdmin** and run the following SQL.
   ```sql
   INSERT INTO public.person(
      first_name, last_name, favorite_color, age, row_version)
      VALUES ('Mickey', 'Mouse', 'Green', 29, now());
   ```

5. Add `.proto` files in **ProtoLibrary** based on the topic schema.
   - Open the Control Center, click **Topics**.
   - Select **source-data-person**, Schema, Value.
   - Copy the schema contents.
   - Create **person.v1.proto** file in **Protos** folder of **ProtoLibrary**.
   - Paste copied schema into the file.
   - Add `option csharp_namespace = "Protos.Source.v1";`
   - Build the solution.
   - Locate the **PersonV1.cs** file in **TransferTest**, obj, Debug, netcoreapp3.1.

6. Run the **TransferTest** app to validate that data is flowing from Postgres to Kafka.
   - Set a breakpoint on the call to `PrintConsumeResult` in `Program.Run_Consumer`.
   - Press F5 to start the debugger.
   - Validate that the message is properly deserialized and displayed in the terminal.

## Sink Connector

1. Open **Mongo Express** and connect to **mongo**.
   - Navigate to http://localhost:8080
   - Username: mongoadmin
   - Password: admin
   - Click **Create Database**
     - Name: sink-database
     - Select sink-database to view
   - Click **Create Collection**
     - Name: person

2. Register MongoDB sink connector.
   - MongoDB connector [documentation](https://docs.mongodb.com/kafka-connector/master/kafka-sink-properties/).
   > **Step 1**: Set the `topics` entry in **register-mongo-sink.json** to `source-data-person`, so that data will flow directly to MongoDB from the topic written to by the Postgres connector.
   - *Option 1*: Upload the connector config `json` file using the **Control Center** user interface.
      - The **register-mongo-sink.json** file is located in the **Connectors** folder.
   - *Option 2*: Run the following `curl` command to upload the config `json` file.

    ```bash
    curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @Connectors/register-mongo-sink.json
    ```

3. In **Mongo Express** validate that a record was added to the `person` collection.
   - After validating tht the record was created, you can delete the record.

4. Write messages to a separate topic for the MongoDB sink database.
   > **Step 2**: Set the `topics` entry in **register-mongo-sink.json** back to `sink-data-person`, so that data will flow to MongoDB through the **TransferTest** app.
   - Delete the **mongo-sink** connector in the Control Center.
   - Then re-upload the **register-mongo-sink.json** file is located in the **Connectors** folder.

5. Run the **TransferTest** app to validate that data is flowing from Postgres to Kafka through an intermediary.
   - Set a breakpoint in `Program.Run_Producer`.
   - Press F5 to start the debugger.
   - Open **pgAdmin** and run the following SQL.
   ```sql
   INSERT INTO public.person(
      first_name, last_name, favorite_color, age, row_version)
      VALUES ('Donald', 'Duck', 'Blue', 30, now());
   ```
   - Open the Control Center to validate that a message was written to the `sink-person-data` topic.
   - Open **Mongo Express** to validate that a record was written to the `person` collection in `sink-database`.

## Worker

