using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace BigData.Server.DataAccessHelper.Utility
{
	public abstract class UQuery
	{
		public abstract string SerializeToJson();

		#region Static Method
		public static UQuery And(params UQuery[] queries)
		{
			var validQueries = queries.Where(q => q != null).Distinct();
			if (validQueries.Count() > 1) {
				return new RelationQuery(RelationType.And, validQueries);
			}
			else {
				return validQueries.FirstOrDefault();
			}
		}

		public static UQuery And(IEnumerable<UQuery> queries)
		{
			var validQueries = queries.Where(q => q != null).Distinct();
			if (validQueries.Count() > 1) {
				return new RelationQuery(RelationType.And, validQueries);
			}
			else {
				return validQueries.FirstOrDefault();
			}
		}

		public static UQuery Or(params UQuery[] queries)
		{
			var validQueries = queries.Where(q => q != null).Distinct();
			if (validQueries.Count() > 1) {
				return new RelationQuery(RelationType.Or, validQueries);
			}
			else {
				return validQueries.FirstOrDefault();
			}
		}

		public static UQuery Or(IEnumerable<UQuery> queries)
		{
			var validQueries = queries.Where(q => q != null).Distinct();
			if (validQueries.Count() > 1) {
				return new RelationQuery(RelationType.Or, validQueries);
			}
			else {
				return validQueries.FirstOrDefault();
			}
		}

		public static UQuery Gt(string key, object value)
		{
			FieldQuery query = new FieldQuery(key, value, QueryType.Greater);
			return query;
		}

		public static UQuery Ge(string key, object value)
		{
			FieldQuery query = new FieldQuery(key, value, QueryType.GreaterOrEqual);
			return query;
		}

		public static UQuery Lt(string key, object value)
		{
			FieldQuery query = new FieldQuery(key, value, QueryType.Less);
			return query;
		}

		public static UQuery Le(string key, object value)
		{
			FieldQuery query = new FieldQuery(key, value, QueryType.LessOrEqual);
			return query;
		}

		public static UQuery Eq(string key, object value)
		{
			FieldQuery query = new FieldQuery(key, value, QueryType.Equal);
			return query;
		}

		public static UQuery NotEq(string key, object value)
		{
			FieldQuery query = new FieldQuery(key, value, QueryType.NotEqual);
			return query;
		}

		public static UQuery Between(string key, object from, object to)
		{
			return UQuery.And(UQuery.Ge(key, from), UQuery.Le(key, to));
		}

		public static UQuery In(string key, Array values)
		{
			FieldQuery query = new FieldQuery(key, values, QueryType.In);
			return query;
		}

		public static UQuery NotIn(string key, Array values)
		{
			FieldQuery query = new FieldQuery(key, values, QueryType.NotIn);
			return query;
		}

		public static UQuery Lk(string key, string value)
		{
			FieldQuery query = new FieldQuery(key, value, QueryType.Like);
			return query;
		}

		public static UQuery Lk(string key, Regex regex)
		{
			FieldQuery query = new FieldQuery(key, regex.ToString(), QueryType.Like);
			return query;
		}
		#endregion

		#region Deserialize
		public static UQuery DeserializeFromJson(string json)
		{
			try {
				string propertyName = JObject.Parse(json).Properties().First().Name;
				if (propertyName == RelationType.And.GetJsonPropertyName() || propertyName == RelationType.Or.GetJsonPropertyName()) {
					return RelationQuery.DeserializeFromJson(json);
				}
				else {
					return FieldQuery.DeserializeFromJson(json);
				}
			}
			catch {
				throw new Exception(string.Format("反序列化Json字符串出错：({0} 不符合规则)", json));
			}
		}
		#endregion
	}
}
