using Confluent.Kafka;
using EventStreamProcessing.Abstractions;
using EventStreamProcessing.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Worker.Handlers;

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
                    services.AddSingleton(brokerOptions);
                    var consumerOptions = hostContext.Configuration
                        .GetSection(nameof(ConsumerOptions))
                        .Get<ConsumerOptions>();
                    services.AddSingleton(consumerOptions);
                    var producerOptions = hostContext.Configuration
                        .GetSection(nameof(ProducerOptions))
                        .Get<ProducerOptions>();
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
                    services.AddSingleton<IEventProcessor>(sp =>
                    {
                        // Get logger
                        var logger = sp.GetRequiredService<ILogger>();

                        // Create consumer
                        var kafkaConsumer = KafkaUtils.CreateConsumer<Protos.Source.v1.person>(
                            brokerOptions, consumerOptions.TopicsList, logger);

                        // Create producer
                        var kafkaProducer = KafkaUtils.CreateProducer<Protos.Sink.v1.Key, Protos.Sink.v1.person>(
                            brokerOptions, logger);

                        // Create event processor with handlers
                        return new KafkaEventProcessor<Ignore, Protos.Source.v1.person, Protos.Sink.v1.Key, Protos.Sink.v1.person>(
                            new KafkaEventConsumer<Ignore, Protos.Source.v1.person>(kafkaConsumer, logger),
                            new KafkaEventProducer<Protos.Sink.v1.Key, Protos.Sink.v1.person>(kafkaProducer, producerOptions.Topic, logger),
                            new TransformHandler(logger));
                    });

                    // Add worker
                    services.AddHostedService<KafkaWorker>();
                });
            return builder;
        }
    }
}