using System;
using System.Collections.Generic;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace TransferTest.Sink
{
    public partial class SinkEnvelope
    {
        [JsonProperty("topic")]
        public string Topic { get; set; }

        [JsonProperty("partition")]
        public long Partition { get; set; }

        [JsonProperty("offset")]
        public long Offset { get; set; }

        [JsonProperty("timestamp")]
        public long Timestamp { get; set; }

        [JsonProperty("timestampType")]
        public string TimestampType { get; set; }

        [JsonProperty("headers")]
        public object[] Headers { get; set; }

        [JsonProperty("key")]
        public Key Key { get; set; }

        [JsonProperty("value")]
        public Value Value { get; set; }

        [JsonProperty("__confluent_index")]
        public long ConfluentIndex { get; set; }
    }

    public partial class Key
    {
        [JsonProperty("person_id")]
        public long PersonId { get; set; }
    }

    public partial class Value
    {
        [JsonProperty("person_id")]
        public long PersonId { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("favorite_color")]
        public string FavoriteColor { get; set; }

        [JsonProperty("age")]
        public long Age { get; set; }
    }

    public partial class SinkEnvelope
    {
        public static SinkEnvelope[] FromJson(string json) => JsonConvert.DeserializeObject<SinkEnvelope[]>(json, TransferTest.Sink.Converter.Settings);
    }

    public static class Serialize
    {
        public static string ToJson(this SinkEnvelope[] self) => JsonConvert.SerializeObject(self, TransferTest.Sink.Converter.Settings);
    }

    internal static class Converter
    {
        public static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
            DateParseHandling = DateParseHandling.None,
            Converters = {
                new IsoDateTimeConverter { DateTimeStyles = DateTimeStyles.AssumeUniversal }
            },
        };
    }
}
