using System.Collections.Generic;
using Confluent.Kafka;

namespace TransferTest
{
    public class BrokerOptions
    {
        public string Brokers { get; set; }
        public SecurityProtocol SecurityProtocol { get; set; }
        public SaslMechanism SaslMechanism { get; set; }
        public string SaslUsername { get; set; }
        public string SaslPassword { get; set; }
        public string SchemaRegistryUrl { get; set; }
        public string SchemaRegistryAuth { get; set; }
    }

    public class ConsumerOptions
    {
        public List<string> TopicsList { get; set; }
    }

    public class ProducerOptions
    {
        public string Topic { get; set; }
    }
}
