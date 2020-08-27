using System.Collections.Generic;
using Confluent.Kafka;

namespace TransferTest
{
    public class ConsumerOptions
    {
        public string Brokers { get; set; }
        public List<string> TopicsList { get; set; }
        public string SchemaRegistryUrl { get; set; }
        public SecurityProtocol SecurityProtocol { get; set; }
        public SaslMechanism SaslMechanism { get; set; }
        public string SaslUsername { get; set; }
        public string SaslPassword { get; set; }
    }

    public class ProducerOptions
    {
        public string Brokers { get; set; }
        public string Topic { get; set; }
        public string SchemaRegistryUrl { get; set; }
    }

}
