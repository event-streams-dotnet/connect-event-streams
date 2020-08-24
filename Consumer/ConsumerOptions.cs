using System.Collections.Generic;

namespace Consumer
{
    public class ConsumerOptions
    {
        public string Brokers { get; set; }
        public List<string> TopicsList { get; set; }
    }
}
