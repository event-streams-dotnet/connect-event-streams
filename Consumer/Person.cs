using Newtonsoft.Json;

namespace Consumer
{
    public class Person
    {
        [JsonRequired]
        [JsonProperty("person_id")]
        public int PersonId { get; set; }

        [JsonRequired]
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("favorite_color")]
        public string FavoriteColor { get; set; }

        [JsonProperty("age")]
        public int Age { get; set; }
    }
}