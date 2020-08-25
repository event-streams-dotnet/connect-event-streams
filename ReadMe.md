# Kafka Connect with Event Streams

Use Kafka Connect to transfer data in real-time between a source and sink with an event stream processor that can perform data transformations.

> **Note**: Here is a blog post explaining the design of  the event stream processing framework: https://blog.tonysneed.com/2020/06/25/event-stream-processing-micro-framework-apache-kafka/.

### Prerequisites

1. Install [Docker Desktop](https://docs.docker.com/desktop/).
   - You will need at least 8 GB of available memory.
2. Open a terminal at the project root and run `docker-compose up --build -d`.
   - To check the running containers run `docker-compose ps`.
   - To bring down the containers run `docker-compose down`.
3. Open a browser to http://localhost:9021/.
   - Verify the cluster is healthy. (This may take a few minutes.)

## Databases

3. Open **Mongo Express** and connect to **mongo**.
   - Navigate to http://localhost:8080
   - Username: mongoadmin
   - Password: admin
   - Click **Create Database**
     - Name: sink-database
     - Select sink-database to view
   - Click **Create Collection**
     - Name: person

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
      person_id integer NOT NULL,
      name text COLLATE pg_catalog."default" NOT NULL,
      favorite_color text COLLATE pg_catalog."default",
      age integer,
      CONSTRAINT person_pkey PRIMARY KEY (person_id)
   )
   ```

3. Register Postgres source connector.
   - Postgres connector [documentation](https://docs.confluent.io/current/connect/debezium-connect-postgres/postgres_source_connector_config.html).

   > **Note**: Rather than executing `curl` commands to register connectors, you can instead upload the registration `json` files using the Control Center user interface.

    ```bash
    curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @Databases/register-postgres.json
    ```
   - Should receive response: `201 Created`.

4. Test the Postgres source connector.
   - Open **pgAdmin** and run the following SQL.
   ```sql
   INSERT INTO public.person(
      person_id, name, favorite_color, age)
      VALUES (1, 'Tony Sneed', 'Green', 29);
   ```
   - Run the Consumer app to read the topic produced by the source connector.
     - Press Enter to accept the default topic.
     - Enter `1` for the schema version. 

   ```bash
   cd Consumer
   dotnet run
   ```

## Sink Connector

1. Register MongoDB sink connector.
   - MongoDB connector [documentation](https://docs.mongodb.com/kafka-connector/master/kafka-sink-properties/).

    ```bash
    curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @Databases/register-mongo.json
    ```
   - Should receive response: `201 Created`.

