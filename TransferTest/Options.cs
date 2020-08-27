using System.Collections.Generic;

namespace TransferTest
{
    public class ConsumerOptions
    {
        public string Brokers { get; set; }
        public List<string> TopicsList { get; set; }
    }

    public class ProducerOptions
    {
        public string Brokers { get; set; }
        public string Topic { get; set; }
        public string SchemaRegistryUrl { get; set; }
    }

}
