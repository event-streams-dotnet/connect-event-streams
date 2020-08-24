using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
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

namespace Consumer
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

            // Get consumer options
            var config = LoadConfiguration();
            var consumerOptions = config
                .GetSection(nameof(ConsumerOptions))
                .Get<ConsumerOptions>();

            // Create topics
            await CreateTopicAsync(consumerOptions.Brokers, consumerOptions.TopicsList);

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

            // Consume events
            switch (version)
            {
                case 1:
                    Run_Consumer<Protos.v1.Key, Protos.v1.Person>(consumerOptions.Brokers, topics, cts.Token);
                    break;
            }
        }

        public static void Run_Consumer<TKey, TValue>(string brokerList, List<string> topics, CancellationToken cancellationToken)
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
                EnablePartitionEof = true
            };

            const int commitPeriod = 5;

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
                .SetKeyDeserializer(new ProtobufDeserializer<TKey>().AsSyncOverAsync())
                .SetValueDeserializer(new ProtobufDeserializer<TValue>().AsSyncOverAsync())
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

        private static void PrintConsumeResult<TKey, TValue>(ConsumeResult<TKey, TValue> consumeResult)
            where TValue : class, IMessage<TValue>, new()
        {
            var key = consumeResult.Message.Key;
            var id = 0;
            var name = string.Empty;
            if (consumeResult.Message.Value is Protos.v1.Person val1)
            {
                id = val1.PersonId;
                name = val1.Name;
            }
            if (consumeResult.Message.Key is Protos.v1.Key key1)
            {
                id = key1.PersonId;
            }
            // if (consumeResult.Message.Value is Protos.v2.HelloReply val2)
            // {
            //     msg = val2.Message;
            //     tmp = val2.TemperatureF != null ? $"at {val2.TemperatureF} degrees" : string.Empty;
            // }
            // if (consumeResult.Message.Value is Protos.v3.HelloReply val3)
            // {
            //     msg = val3.Message;
            //     tmp = val3.TemperatureF != null ? $"at {val3.TemperatureF} degrees" : string.Empty;
            //     ts = val3.DateTimeStamp;
            // }
            // if (consumeResult.Message.Value is Protos.v4.HelloReply val4)
            // {
            //     msg = val4.Message;
            //     ts = val4.DateTimeStamp;
            // }
            // if (consumeResult.Message.Value is Protos.v5.HelloReply val5)
            // {
            //     msg = val5.Message;
            //     var dt = DateTime.SpecifyKind(DateTime.Parse(val5.DateTimeStamp), DateTimeKind.Utc);
            //     ts = GoogleTimestamp.FromDateTime(dt);
            // }
            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {id} (key) {name}");
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
