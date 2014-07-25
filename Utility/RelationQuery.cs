using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace BigData.Server.DataAccessHelper.Utility
{
	class RelationQuery : UQuery
	{
		public RelationType Relation { get; private set; }
		public UQuery[] Queries { get; private set; }

		public RelationQuery(RelationType relation, IEnumerable<UQuery> queries)
		{
			List<UQuery> queryList = new List<UQuery>();
			queries.Where(item => item != null).ToList().ForEach(q => {
				if (q is RelationQuery) {
					RelationQuery query = q as RelationQuery;
					if (query.Relation == relation) {
						queryList.AddRange(query.Queries);
					}
					else {
						queryList.Add(q);
					}
				}
				else {
					queryList.Add(q);
				}
			});
			this.Relation = relation;
			this.Queries = queryList.Distinct().ToArray();
		}

		public override string ToString()
		{
			return this.SerializeToJson();
		}

		public override bool Equals(object obj)
		{
			if (obj == null) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj is RelationQuery) {
				RelationQuery other = obj as RelationQuery;
				if (this.Relation == other.Relation
					&& this.Queries.Count(q => other.Queries.Contains(q)) == this.Queries.Count()
					&& other.Queries.Count(q => this.Queries.Contains(q)) == other.Queries.Count()) {
					return true;
				}
			}
			return false;
		}

		public override int GetHashCode()
		{
			return this.SerializeToJson().GetHashCode();
		}

		#region Serialize & Deserialize
		public override string SerializeToJson()
		{
			if (this.Queries.Length > 1) {
				JObject obj = new JObject();
				JArray array = new JArray();
				foreach (UQuery query in Queries) {
					array.Add(JObject.Parse(query.SerializeToJson()));
				}
				JProperty property = new JProperty(this.Relation.GetJsonPropertyName(), array);
				obj.Add(property);
				return obj.ToString(Formatting.None);
			}
			else {
				return this.Queries[0].SerializeToJson();
			}
		}

		public new static RelationQuery DeserializeFromJson(string json)
		{
			RelationQuery query = null;
			JObject obj = JObject.Parse(json);
			string propertyName = obj.Properties().First().Name;
			JArray propertyValue = obj.Properties().First().Value as JArray;
			IEnumerable<UQuery> queries = propertyValue.Select(item => UQuery.DeserializeFromJson(item.ToString()));
			RelationType type = RelationTypeExtensions.GetTypeByPropertyName(propertyName);
			switch (type) {
				case RelationType.And:
					query = new RelationQuery(RelationType.And, queries);
					break;
				case RelationType.Or:
					query = new RelationQuery(RelationType.Or, queries);
					break;
			}
			return query;
		}
		#endregion
	}
}
