namespace TransferTest.Source
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;

    public partial class Key
    {
        [JsonProperty("person_id")]
        public long PersonId { get; set; }
    }

    public partial class Value
    {
        [JsonProperty("before")]
        public object Before { get; set; }

        [JsonProperty("after")]
        public After After { get; set; }

        [JsonProperty("source")]
        public Source Source { get; set; }

        [JsonProperty("op")]
        public string Op { get; set; }

        [JsonProperty("ts_ms")]
        public long TsMs { get; set; }

        [JsonProperty("transaction")]
        public object Transaction { get; set; }
    }

    public partial class After
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

    public partial class Source
    {
        [JsonProperty("version")]
        public string Version { get; set; }

        [JsonProperty("connector")]
        public string Connector { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("ts_ms")]
        public long TsMs { get; set; }

        [JsonProperty("snapshot")]
        [JsonConverter(typeof(ParseStringConverter))]
        public bool Snapshot { get; set; }

        [JsonProperty("db")]
        public string Db { get; set; }

        [JsonProperty("schema")]
        public string Schema { get; set; }

        [JsonProperty("table")]
        public string Table { get; set; }

        [JsonProperty("txId")]
        public long TxId { get; set; }

        [JsonProperty("lsn")]
        public long Lsn { get; set; }

        [JsonProperty("xmin")]
        public object Xmin { get; set; }
    }

    public partial class SourceEnvelope
    {
        public static SourceEnvelope[] FromJson(string json) => JsonConvert.DeserializeObject<SourceEnvelope[]>(json, TransferTest.Source.Converter.Settings);
    }

    public static class Serialize
    {
        public static string ToJson(this SourceEnvelope[] self) => JsonConvert.SerializeObject(self, TransferTest.Source.Converter.Settings);
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

    internal class ParseStringConverter : JsonConverter
    {
        public override bool CanConvert(Type t) => t == typeof(bool) || t == typeof(bool?);

        public override object ReadJson(JsonReader reader, Type t, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null) return null;
            var value = serializer.Deserialize<string>(reader);
            bool b;
            if (Boolean.TryParse(value, out b))
            {
                return b;
            }
            throw new Exception("Cannot unmarshal type bool");
        }

        public override void WriteJson(JsonWriter writer, object untypedValue, JsonSerializer serializer)
        {
            if (untypedValue == null)
            {
                serializer.Serialize(writer, null);
                return;
            }
            var value = (bool)untypedValue;
            var boolString = value ? "true" : "false";
            serializer.Serialize(writer, boolString);
            return;
        }

        public static readonly ParseStringConverter Singleton = new ParseStringConverter();
    }
}
