using EventStreamProcessing.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

// Alias
using Result = Confluent.Kafka.DeliveryResult<Protos.Sink.v1.Key, Protos.Sink.v1.person>;

namespace Worker
{
    public class KafkaWorker : BackgroundService
    {
        private readonly IEventProcessorWithResult<Result> eventProcessor;
        private readonly ILogger logger;

        public KafkaWorker(IEventProcessorWithResult<Result> eventProcessor, ILogger logger)
        {
            this.eventProcessor = eventProcessor;
            this.logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                logger.LogInformation($"Worker processing event at: {DateTimeOffset.Now}");

                // Process event
                var deliveryResult = await eventProcessor.ProcessWithResult(cancellationToken);
                if (deliveryResult != null)
                    logger.LogInformation($"delivered to: {deliveryResult.TopicPartitionOffset}");
            }
        }
    }
}
