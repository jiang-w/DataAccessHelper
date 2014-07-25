using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace BigData.Server.DataAccessHelper.Utility
{
	public class SortBuilder
	{
		private List<SortField> _sortList;

		public SortField[] SortFields { get { return _sortList.ToArray(); } }

		public SortBuilder()
		{
			_sortList = new List<SortField>();
		}

		public SortBuilder Ascending(params string[] keys)
		{
			if (keys != null) {
				keys.Where(key => !string.IsNullOrWhiteSpace(key)).ToList().ForEach(key => {
					if (Contains(key)) {
						Remove(key);
					}
					_sortList.Add(new SortField(key, SortMode.ASC));
				});
			}
			return this;
		}

		public SortBuilder Descending(params string[] keys)
		{
			if (keys != null) {
				keys.Where(key => !string.IsNullOrWhiteSpace(key)).ToList().ForEach(key => {
					if (Contains(key)) {
						Remove(key);
					}
					_sortList.Add(new SortField(key, SortMode.DESC));
				});
			}
			return this;
		}

		private bool Contains(string key)
		{
			if (_sortList.Where(item => item.FieldName.Equals(key, StringComparison.InvariantCultureIgnoreCase)).Count() > 0)
				return true;
			else
				return false;
		}

		private void Remove(string key)
		{
			do {
				var items = _sortList.Where(item => item.FieldName.Equals(key, StringComparison.InvariantCultureIgnoreCase));
				if (items.Count() > 0)
					_sortList.Remove(items.First());
				else
					break;
			}
			while (true);
		}

		#region Serialize & Deserialize
		public string SerializeToJson()
		{
			var objs = _sortList.Select(sort => new JObject(new JProperty(sort.FieldName, sort.SortMode)));
			if (objs.Count() > 1) {
				JArray array = new JArray(objs.ToArray());
				return array.ToString(Formatting.None);
			}
			else if (objs.Count() == 1) {
				return objs.First().ToString(Formatting.None);
			}
			else {
				return new JObject().ToString(Formatting.None);
			}
		}

		public static SortField[] DeserializeFromJson(string json)
		{
			List<SortField> sortList = new List<SortField>();
			JToken token = JToken.Parse(json);
			if (token.Type == JTokenType.Array) {
				JArray array = token as JArray;
				sortList.AddRange(array.Select(item => {
					JObject obj = JObject.Parse(item.ToString());
					JProperty p = obj.Properties().First();
					return new SortField(p.Name, (SortMode)p.Value.Value<int>());
				})
					);
			}
			else {
				JObject obj = token as JObject;
				JProperty p = obj.Properties().First();
				sortList.Add(new SortField(p.Name, (SortMode)p.Value.Value<int>()));
			}
			return sortList.ToArray();
		}
		#endregion

		public override string ToString()
		{
			return SerializeToJson();
		}
	}
}
