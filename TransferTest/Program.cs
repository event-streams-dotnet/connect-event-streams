using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using demo_source.@public.person.Value;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TransferTest
{
    class Program
    {
        public static void Main(string[] args)
        {
            // Prevent the process from terminating
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            // Get consumer and producer options
            var config = LoadConfiguration();
            var consumerOptions = config
                .GetSection(nameof(ConsumerOptions))
                .Get<ConsumerOptions>();
            var producerOptions = config
                .GetSection(nameof(ProducerOptions))
                .Get<ProducerOptions>();

            // Create topics
            // await CreateTopicAsync(consumerOptions.Brokers, consumerOptions.TopicsList);

            // Confirm topic
            Console.WriteLine($"Press Ctrl-C to quit.");
            Console.WriteLine($"\nDefault topic: {consumerOptions.TopicsList[0]}");
            Console.WriteLine("> Confirm: <Enter>, New value<Enter>");
            var topic = Console.ReadLine();
            if (topic.Length == 0)
                topic = consumerOptions.TopicsList[0];
            var topics = new List<string> { topic };
            Console.WriteLine($"Topic: {topic}");

            // Get schema version number
            Console.WriteLine("\nEnter schema version number:");
            if (!int.TryParse(Console.ReadLine(), out int version))
                return;
            Console.WriteLine($"Schema version: {version}");

            switch (version)
            {
                case 1:
                    var x = Run_Consumer<Ignore, person>(consumerOptions.Brokers, topics, consumerOptions.SecurityProtocol,
                        consumerOptions.SaslMechanism, consumerOptions.SaslUsername, consumerOptions.SaslPassword,
                        consumerOptions.SchemaRegistryUrl, cts.Token);
                    // await Run_Producer<Sink.Key, Sink.Value>(producerOptions.Brokers, producerOptions.Topic, producerOptions.SchemaRegistryUrl, message);
                    break;
            }
        }

        public static ConsumeResult<TKey, TValue> Run_Consumer<TKey, TValue>(string brokerList, List<string> topics, 
            SecurityProtocol securityProtocol, SaslMechanism saslMechanism, string saslUsername, string saslPassword,
            string schemaRegistryUrl, CancellationToken cancellationToken)
            // where TKey : class, new()
            where TValue : class, new()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = "csharp-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                SecurityProtocol = securityProtocol,
                SaslMechanism = saslMechanism,
                SaslUsername = saslUsername,
                SaslPassword = saslPassword
            };
            const int commitPeriod = 5;
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl,
                BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                BasicAuthUserInfo = "XT3S2ODODDUVGA4P:oMn9Z88WzhcfXlvYtTw++tHSYcq4/uObsob3fSH4x8hZXKiEKVUZNovdcATBvMb/"
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer = new ConsumerBuilder<TKey, TValue>(config)
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]\n");
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]");
                })
                // Set value Protobuf deserializer
                // .SetKeyDeserializer(new AvroDeserializer<TKey>(schemaRegistry).AsSyncOverAsync())
                .SetValueDeserializer(new AvroDeserializer<TValue>(schemaRegistry).AsSyncOverAsync())
                .Build())
            {
                consumer.Subscribe(topics);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }

                            PrintConsumeResult(consumeResult);

                            if (consumeResult.Offset % commitPeriod == 0)
                            {
                                try
                                {
                                    consumer.Commit(consumeResult);
                                }
                                catch (KafkaException e)
                                {
                                    Console.WriteLine($"Commit error: {e.Error.Reason}");
                                }
                            }
                            return consumeResult;
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
                return null;
            }
        }

        private static async Task Run_Producer<TKey, TValue>(string brokerList, string topicName, 
            string schemaRegistryUrl, Message<TKey, TValue> message)
            where TKey : class, new()
            where TValue : class, new()
        {
            var config = new ProducerConfig { BootstrapServers = brokerList };
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryUrl };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new ProducerBuilder<TKey, TValue>(config)
                .SetKeySerializer(new AvroSerializer<TKey>(schemaRegistry))
                .SetValueSerializer(new AvroSerializer<TValue>(schemaRegistry))
                .Build())
            {
                try
                {
                    // Note: Awaiting the asynchronous produce request below prevents flow of execution
                    // from proceeding until the acknowledgement from the broker is received (at the 
                    // expense of low throughput).
                    var deliveryReport = await producer.ProduceAsync(topicName, message);

                    Console.WriteLine($"delivered to: {deliveryReport.TopicPartitionOffset} for producer: {producer.Name}");
                }
                catch (ProduceException<int, TValue> e)
                {
                    Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                }

                // Since we are producing synchronously, at this point there will be no messages
                // in-flight and no delivery reports waiting to be acknowledged, so there is no
                // need to call producer.Flush before disposing the producer.
            }
        }
        private static void PrintConsumeResult<TKey, TValue>(ConsumeResult<TKey, TValue> consumeResult)
            // where TKey : class, new()
            where TValue : class, new()
        {
            long key = 0;
            long id = 0;
            var name = string.Empty;
            var favColor = string.Empty;
            int? age = 0;
            DateTime ts = DateTime.MinValue;
            if (consumeResult.Message.Value is person val1)
            {
                id = val1.person_id;
                name = val1.name;
                favColor = val1.favorite_color;
                age = val1.age;
                ts = val1.created_on;
            }
            // if (consumeResult.Message.Key is Source.Key key1)
            // {
            //     key = key1.PersonId;
            // }
            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: Key: {key}, Id: {id}, Name: {name}, Fav Color: {favColor}, Age: {age} Created On: {ts.ToShortTimeString()}");
        }

        private static TValue CreateMessageValue<TValue>(string msg)
            where TValue : class, new()
        {
            int? tmp = new Random().Next(-32, 100);
            // var ts = GoogleTimestamp.FromDateTime(DateTime.UtcNow);
            var val = new TValue();
            // if (val is IHelloReply val1)
            // {
            //     val1.Message = msg;
            // };
            // if (val is IHelloReply_2 val2)
            // {
            //     val2.TemperatureF = tmp;
            // };
            // if (val is IHelloReply_3 val3)
            // {
            //     val3.TemperatureF = tmp;
            //     val3.DateTimeStamp = ts;
            // };
            // if (val is IHelloReply_4 val4)
            // {
            //     val4.DateTimeStamp = ts;
            // };
            // if (val is IHelloReply_5 val5)
            // {
            //     val5.DateTimeStamp = DateTime.UtcNow.ToLongTimeString();
            // };
            return val;
        }

        static async Task CreateTopicAsync(string brokerList, List<string> topics)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = brokerList }).Build())
            {
                try
                {
                    var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));
                    foreach (var topic in topics)
                    {
                        if (!meta.Topics.Exists(t => t.Topic == topic))
                        {
                            var topicSpecs = topics.Select(topicName =>
                                new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = 1 });
                            await adminClient.CreateTopicsAsync(topicSpecs);
                        }
                    }
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
        }

        private static IConfiguration LoadConfiguration()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables();
            return builder.Build();
        }
    }
}
