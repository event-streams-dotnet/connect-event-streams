using EventStreamProcessing.Abstractions;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace Worker.Handlers
{
    public class TransformHandler : MessageHandler
    {
        private readonly ILogger logger;

        public TransformHandler(ILogger logger)
        {
            this.logger = logger;
        }

        public override async Task<Message> HandleMessage(Message sourceMessage)
        {
            // Convert message from source to sink format
            var message = (Message<Confluent.Kafka.Ignore, Protos.Source.v1.person>)sourceMessage;
            var key = new Protos.Sink.v1.Key
            {
                PersonId = message.Value.PersonId
            };
            var value = new Protos.Sink.v1.person
            {
                PersonId = message.Value.PersonId,
                Name = $"{message.Value.FirstName} {message.Value.LastName}",
                FavoriteColor = message.Value.FavoriteColor,
                Age = message.Value.Age,
                RowVersion = message.Value.RowVersion
            };

            // Call next handler
            var sinkMessage = new Message<Protos.Sink.v1.Key, Protos.Sink.v1.person>(key, value);
            logger.LogInformation($"Transform handler: {sinkMessage.Key} {sinkMessage.Value}");
            return await base.HandleMessage(sinkMessage);
        }
    }
}