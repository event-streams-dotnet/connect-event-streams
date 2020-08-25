using Newtonsoft.Json;

namespace Consumer
{
    public class Key
    {
        [JsonRequired]
        [JsonProperty("person_id")]
        public int PersonId { get; set; }
    }
}