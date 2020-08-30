using Confluent.Kafka;
using EventStreamProcessing.Abstractions;
using EventStreamProcessing.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Worker.Handlers;

// Aliases
using SourcePerson = Protos.Source.v1.person;
using SinkKey = Protos.Sink.v1.Key;
using SinkPerson = Protos.Sink.v1.person;
using Result = Confluent.Kafka.DeliveryResult<Protos.Sink.v1.Key, Protos.Sink.v1.person>;

namespace Worker
{
    class Program
    {
        private static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        private static IHostBuilder CreateHostBuilder(string[] args)
        {
            var builder = Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    // Add options
                    var brokerOptions = hostContext.Configuration
                        .GetSection(nameof(BrokerOptions))
                        .Get<BrokerOptions>();
                    var consumerOptions = hostContext.Configuration
                        .GetSection(nameof(ConsumerOptions))
                        .Get<ConsumerOptions>();
                    var producerOptions = hostContext.Configuration
                        .GetSection(nameof(ProducerOptions))
                        .Get<ProducerOptions>();
                    services.AddSingleton(brokerOptions);
                    services.AddSingleton(consumerOptions);
                    services.AddSingleton(producerOptions);

                    // Add logger
                    services.AddSingleton<ILogger>(sp =>
                    {
                        var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger<KafkaWorker>();
                        logger.LogInformation($"Hosting Environment: {hostContext.HostingEnvironment.EnvironmentName}");
                        logger.LogInformation($"Consumer Brokers: {brokerOptions.Brokers}");
                        logger.LogInformation($"Consumer Brokers: {brokerOptions.Brokers}");
                        return logger;
                    });

                    // Add event processor
                    services.AddSingleton<IEventProcessorWithResult<Result>>(sp =>
                    {
                        // Get logger
                        var logger = sp.GetRequiredService<ILogger>();

                        // Create consumer, producer
                        var kafkaConsumer = KafkaUtils.CreateConsumer<SourcePerson>(
                            brokerOptions, consumerOptions.TopicsList, logger);
                        var kafkaProducer = KafkaUtils.CreateProducer<SinkKey, SinkPerson>(
                            brokerOptions, logger);

                        // Return event processor using async producer
                        return new KafkaEventProcessorWithResult<Ignore, SourcePerson, SinkKey, SinkPerson>(
                            new KafkaEventConsumer<Ignore, SourcePerson>(kafkaConsumer, logger),
                            new KafkaEventProducerAsync<SinkKey, SinkPerson>(kafkaProducer, producerOptions.Topic),
                            new TransformHandler(logger));
                    });

                    // Add worker
                    services.AddHostedService<KafkaWorker>();
                });
            return builder;
        }
    }
}