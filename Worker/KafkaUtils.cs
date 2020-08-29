using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.SyncOverAsync;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Confluent.SchemaRegistry;

namespace Worker
{
    public static class KafkaUtils
    {
        public static IConsumer<Ignore, TValue> CreateConsumer<TValue>(
            BrokerOptions brokerOptions, List<string> topics, ILogger logger)
            where TValue : class, IMessage<TValue>, new()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = brokerOptions.Brokers,
                GroupId = "sample-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                ReconnectBackoffMs = 30000, // 30 seconds
                ReconnectBackoffMaxMs = 180000, // 3 minutes
                // SecurityProtocol = brokerOptions.SecurityProtocol,
                // SaslMechanism = brokerOptions.SaslMechanism,
                // SaslUsername = brokerOptions.SaslUsername,
                // SaslPassword = brokerOptions.SaslPassword,
            };

            var consumer = new ConsumerBuilder<Ignore, TValue>(config)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => logger.LogInformation($"Error: {e.Reason}"))
                // .SetStatisticsHandler((_, json) => logger.LogInformation($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    logger.LogInformation($"Assigned partitions: [{string.Join(", ", partitions)}]");
                    // possibly manually specify start offsets or override the partition assignment provided by
                    // the consumer group by returning a list of topic/partition/offsets to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    logger.LogInformation($"Revoking assignment: [{string.Join(", ", partitions)}]");
                })
                .SetValueDeserializer(new ProtobufDeserializer<TValue>().AsSyncOverAsync())
                .Build();

            consumer.Subscribe(topics);
            return consumer;
        }

        public static IProducer<TKey, TValue> CreateProducer<TKey, TValue>(
            BrokerOptions brokerOptions, ILogger logger)
            where TKey : class, IMessage<TKey>, new()
            where TValue : class, IMessage<TValue>, new()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = brokerOptions.Brokers,
                // EnableIdempotence = true,
                // MessageSendMaxRetries = 10000000,
                // SecurityProtocol = brokerOptions.SecurityProtocol,
                // SaslMechanism = brokerOptions.SaslMechanism,
                // SaslUsername = brokerOptions.SaslUsername,
                // SaslPassword = brokerOptions.SaslPassword,
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = brokerOptions.SchemaRegistryUrl,
                // BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                // BasicAuthUserInfo = brokerOptions.SchemaRegistryAuth,
            };

            var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
            var producer = new ProducerBuilder<TKey, TValue>(config)
                .SetKeySerializer(new ProtobufSerializer<TKey>(schemaRegistry))
                .SetValueSerializer(new ProtobufSerializer<TValue>(schemaRegistry))
                .Build();

            return producer;
        }
    }
}
