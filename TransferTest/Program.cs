using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Google.Protobuf;
using GoogleTimestamp = Google.Protobuf.WellKnownTypes.Timestamp;
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
        public static async Task Main(string[] args)
        {
            // Prevent the process from terminating
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            // Get consumer and producer options
            var config = LoadConfiguration();
            var brokerOptions = config
                .GetSection(nameof(BrokerOptions))
                .Get<BrokerOptions>();
            var consumerOptions = config
                .GetSection(nameof(ConsumerOptions))
                .Get<ConsumerOptions>();
            var producerOptions = config
                .GetSection(nameof(ProducerOptions))
                .Get<ProducerOptions>();

            // Confirm topic
            Console.WriteLine($"Press Ctrl-C to quit.");
            Console.WriteLine($"\nDefault topic: {consumerOptions.TopicsList[0]}");
            var consumerTopics = new List<string> { consumerOptions.TopicsList[0] };

            await Run_Consumer<Protos.Sink.v1.Key, Protos.Source.v1.person>(brokerOptions.Brokers, consumerTopics, producerOptions.Topic,
                brokerOptions.SecurityProtocol, brokerOptions.SaslMechanism, brokerOptions.SaslUsername, brokerOptions.SaslPassword,
                brokerOptions.SchemaRegistryUrl, brokerOptions.SchemaRegistryAuth ,cts.Token);
        }

        public static async Task Run_Consumer<TKey, TValue>(string brokerList, List<string> consumerTopics, string producerTopic,
            SecurityProtocol securityProtocol, SaslMechanism saslMechanism, string saslUsername, string saslPassword,
            string schemaRegistryUrl, string basicAuthUserInfo, CancellationToken cancellationToken)
            where TKey : class, IMessage<TKey>, new()
            where TValue : class, IMessage<TValue>, new()
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
                // SecurityProtocol = securityProtocol,
                // SaslMechanism = saslMechanism,
                // SaslUsername = saslUsername,
                // SaslPassword = saslPassword
            };

            const int commitPeriod = 5;

            using (var consumer = new ConsumerBuilder<Ignore, TValue>(config)
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
                // .SetKeyDeserializer(new ProtobufDeserializer<TKey>().AsSyncOverAsync())
                .SetValueDeserializer(new ProtobufDeserializer<TValue>().AsSyncOverAsync())
                .Build())
            {
                consumer.Subscribe(consumerTopics);

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

                            await Run_Producer<TKey, TValue>(brokerList, producerTopic, securityProtocol, saslMechanism, 
                                saslUsername, saslPassword, schemaRegistryUrl, basicAuthUserInfo, consumeResult.Message);
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
            }
        }

        private static async Task Run_Producer<TKey, TValue>(string brokerList, string topicName, 
            SecurityProtocol securityProtocol, SaslMechanism saslMechanism, string saslUsername, string saslPassword,
            string schemaRegistryUrl, string basicAuthUserInfo, Message<Ignore, TValue> consumerMessage)
            where TKey : class, IMessage<TKey>, new()
            where TValue : class, IMessage<TValue>, new()
        {
            var message = CreateMessage<TKey, TValue>(consumerMessage.Value);
            var config = new ProducerConfig
            {
                BootstrapServers = brokerList,
                // SecurityProtocol = securityProtocol,
                // SaslMechanism = saslMechanism,
                // SaslUsername = saslUsername,
                // SaslPassword = saslPassword
            };
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl,
                // BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                // BasicAuthUserInfo = basicAuthUserInfo
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new ProducerBuilder<TKey, TValue>(config)
                .SetKeySerializer(new ProtobufSerializer<TKey>(schemaRegistry))
                .SetValueSerializer(new ProtobufSerializer<TValue>(schemaRegistry))
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
            var first_name = string.Empty;
            var last_name = string.Empty;
            var favColor = string.Empty;
            int age = 0;
            GoogleTimestamp ts = new GoogleTimestamp();
            if (consumeResult.Message.Value is Protos.Source.v1.person val1)
            {
                id = val1.PersonId;
                first_name = val1.FirstName;
                last_name = val1.LastName;
                favColor = val1.FavoriteColor;
                age = val1.Age;
                ts = val1.RowVersion;
            }
            // if (consumeResult.Message.Key is Source.Key key1)
            // {
            //     key = key1.PersonId;
            // }
            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: Key: {key}, " +
                $"Id: {id}, Name: {first_name} {last_name}, Fav Color: {favColor}, Age: {age} RowVersion: {ts}");
        }

        static Message<TKey, TValue> CreateMessage<TKey, TValue>(TValue value)
            where TKey : class, IMessage<TKey>, new()
            where TValue : class, IMessage<TValue>, new()
        {
            var key = new TKey();
            if (value is Protos.Source.v1.person val1)
            {
                if (key is Protos.Sink.v1.Key key1)
                {
                    key1.PersonId = val1.PersonId;
                }
            }
            var message = new Message<TKey, TValue>
            {
                Key = key,
                Value = value
            };
            return message;
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
            var environmentName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile($"appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile($"appsettings.{environmentName}.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables();
            return builder.Build();
        }
    }
}
