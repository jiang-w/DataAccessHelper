using System;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace BigData.Server.DataAccessHelper.Utility
{
	class FieldQuery : UQuery
	{
		public string Key { get; private set; }
		public object Value { get; private set; }
		public QueryType Type { get; private set; }

		public FieldQuery(string key, object value, QueryType type)
		{
			this.Key = key;
			this.Value = value;
			this.Type = type;
		}

		public override string ToString()
		{
			return this.SerializeToJson();
		}

		public override bool Equals(object obj)
		{
			if (obj == null) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj is FieldQuery) {
				FieldQuery other = obj as FieldQuery;
				return this.Type == other.Type && this.Key == other.Key && Equals(this.Value, other.Value);
			}
			else {
				return false;
			}
		}

		public override int GetHashCode()
		{
			return this.SerializeToJson().GetHashCode();
		}

		#region Serialize & Deserialize
		public override string SerializeToJson()
		{
			StringBuilder jsonBuilder = new StringBuilder();
			StringWriter sw = new StringWriter(jsonBuilder);
			using (JsonWriter writer = new JsonTextWriter(sw) {
				Formatting = Formatting.None,
				DateFormatString = "yyyy-MM-ddThh:mm:ss"
			}) {
				//writer.DateFormatHandling = DateFormatHandling.IsoDateFormat;
				writer.WriteStartObject();
				writer.WritePropertyName(this.Key);
				if (this.Type != QueryType.Equal) {
					writer.WriteStartObject();
					writer.WritePropertyName(this.Type.GetJsonPropertyName());
					WriterJsonValue(writer, this.Value);
					writer.WriteEndObject();
				}
				else {
					WriterJsonValue(writer, this.Value);
				}
				writer.WriteEndObject();
			}
			return jsonBuilder.ToString();
		}

		public new static FieldQuery DeserializeFromJson(string json)
		{
			FieldQuery query = null;
			JObject obj = JObject.Parse(json);
			string key = obj.Properties().First().Name;
			var value = DeserializeJToken(obj.Properties().First().Value);
			QueryType type = QueryType.Equal;
			if (value is JObject) {
				string relationStr = (value as JObject).Properties().First().Name;
				type = QueryTypeExtensions.GetTypeByPropertyName(relationStr);
				value = DeserializeJToken((value as JObject).Property(relationStr).Value);
			}
			query = new FieldQuery(key, value, type);
			return query;
		}

		private static void WriterJsonValue(JsonWriter writer, object value)
		{
			if (value is Array) {
				writer.WriteStartArray();
				foreach (var v in (Array)value) {
					writer.WriteValue(v);
				}
				writer.WriteEndArray();
			}
			else {
				writer.WriteValue(value);
			}
		}

		private static object DeserializeJToken(JToken token)
		{
			switch (token.Type) {
				case JTokenType.Array:
					JArray array = token as JArray;
					var values = array.Select(item => DeserializeJToken(item));

					JTokenType firstItemType = array.First.Type;
					foreach (var item in array) {
						if (item.Type != firstItemType)
							return values.ToArray();
					}
					switch (firstItemType) {
						case JTokenType.Boolean:
							return values.Select(item => (bool)item).ToArray();
						case JTokenType.Bytes:
							return values.Select(item => (byte[])item).ToArray();
						case JTokenType.Date:
							return values.Select(item => (DateTime)item).ToArray();
						case JTokenType.Float:
							return values.Select(item => (double)item).ToArray();
						case JTokenType.Integer:
							return values.Select(item => (int)item).ToArray();
						case JTokenType.String:
							return values.Select(item => (string)item).ToArray();
						case JTokenType.TimeSpan:
							return values.Select(item => (TimeSpan)item).ToArray();
						default:
							return values.ToArray();
					}
				case JTokenType.Boolean:
					return token.Value<bool>();
				case JTokenType.Bytes:
					return token.Value<byte[]>();
				case JTokenType.Date:
					return token.Value<DateTime>();
				case JTokenType.Float:
					return token.Value<double>();
				case JTokenType.Integer:
					return token.Value<int>();
				case JTokenType.Null:
					return null;
				case JTokenType.String:
					return token.Value<string>();
				case JTokenType.TimeSpan:
					return token.Value<TimeSpan>();
				case JTokenType.Object:
					return token.Value<JObject>();
				default:
					return token.ToString();
			}
		}
		#endregion
	}
}