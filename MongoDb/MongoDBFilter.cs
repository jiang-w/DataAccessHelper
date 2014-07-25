using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace BigData.Server.DataAccessHelper.MongoDb
{
	public class MongoDBFilter
	{
		#region 字段属性
		internal QueryDocument QueryDoc { get; private set; }
		#endregion

		#region 构造函数
		public MongoDBFilter()
		{
			QueryDoc = new QueryDocument();
		}

		private MongoDBFilter(QueryDocument doc)
		{
			QueryDoc = doc;
		}
		#endregion

		#region 类静态方法（组合过滤条件）

		/// <summary>
		/// 将一组过滤条件进行And操作，组合为一个条件
		/// </summary>
		/// <param name="filters"></param>
		/// <returns></returns>
		public static MongoDBFilter And(params MongoDBFilter[] filters)
		{
			IEnumerable<IMongoQuery> queries = filters.Where(item => item != null)
				.Select(item => item.QueryDoc as IMongoQuery);
			if (queries.Count() == 0)
				return null;
			QueryDocument doc = Query.And(queries) as QueryDocument;
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 将一组过滤条件进行And操作，组合为一个条件
		/// </summary>
		/// <param name="list"></param>
		/// <returns></returns>
		public static MongoDBFilter And(List<MongoDBFilter> list)
		{
			IEnumerable<IMongoQuery> queries = list.Select(item => item.QueryDoc as IMongoQuery);
			QueryDocument doc = Query.And(queries) as QueryDocument;
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 对一组过滤条件进行Or操作，组合为一个条件
		/// </summary>
		/// <param name="filters"></param>
		/// <returns></returns>
		public static MongoDBFilter Or(params MongoDBFilter[] filters)
		{
			IEnumerable<IMongoQuery> queries = filters.Where(item => item != null)
				.Select(item => item.QueryDoc as IMongoQuery);
			if (queries.Count() == 0)
				return null;
			QueryDocument doc = Query.Or(queries) as QueryDocument;
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 对一组过滤条件进行Or操作，组合为一个条件
		/// </summary>
		/// <param name="list"></param>
		/// <returns></returns>
		public static MongoDBFilter Or(List<MongoDBFilter> list)
		{
			IEnumerable<IMongoQuery> queries = list.Select(item => item.QueryDoc as IMongoQuery);
			QueryDocument doc = Query.Or(queries) as QueryDocument;
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 创建大于条件
		/// </summary>
		/// <param name="field">字段名</param>
		/// <param name="value">值</param>
		/// <returns></returns>
		public static MongoDBFilter Gt(string field, object value)
		{
			QueryDocument doc = new QueryDocument(field, new BsonDocument("$gt", BsonValue.Create(value)));
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 创建大于等于条件
		/// </summary>
		/// <param name="field">字段名</param>
		/// <param name="value">值</param>
		/// <returns></returns>
		public static MongoDBFilter Ge(string field, object value)
		{
			QueryDocument doc = new QueryDocument(field, new BsonDocument("$gte", BsonValue.Create(value)));
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 创建小于条件
		/// </summary>
		/// <param name="field"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public static MongoDBFilter Lt(string field, object value)
		{
			QueryDocument doc = new QueryDocument(field, new BsonDocument("$lt", BsonValue.Create(value)));
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 创建小于等于条件
		/// </summary>
		/// <param name="field"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public static MongoDBFilter Le(string field, object value)
		{
			QueryDocument doc = new QueryDocument(field, new BsonDocument("$lte", BsonValue.Create(value)));
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 创建一个Between关系的过滤条件
		/// </summary>
		/// <param name="field"></param>
		/// <param name="value1"></param>
		/// <param name="value2"></param>
		/// <returns></returns>
		public static MongoDBFilter Between(string field, object value1, object value2)
		{
			Dictionary<string, object> dict = new Dictionary<string, object>();
			dict.Add("$gte", value1);
			dict.Add("$lte", value2);
			QueryDocument doc = new QueryDocument(dict);
			return new MongoDBFilter(new QueryDocument(field, doc));
		}

		/// <summary>
		/// 创建一个In关系的过滤条件
		/// </summary>
		/// <param name="field"></param>
		/// <param name="values"></param>
		/// <returns></returns>
		public static MongoDBFilter In(string field, Array values)
		{
			QueryDocument doc = new QueryDocument(field, new BsonDocument("$in", new BsonArray(values)));
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 创建一个Not In关系的过滤条件
		/// </summary>
		/// <param name="field"></param>
		/// <param name="values"></param>
		/// <returns></returns>
		public static MongoDBFilter NotIn(string field, Array values)
		{
			QueryDocument doc = new QueryDocument(field, new BsonDocument("$nin", new BsonArray(values)));
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 创建一个等于关系的过滤条件
		/// </summary>
		/// <param name="field"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public static MongoDBFilter Eq(string field, object value)
		{
			QueryDocument doc = new QueryDocument(field, BsonValue.Create(value));
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 创建一个不等于关系的过滤条件
		/// </summary>
		/// <param name="field"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public static MongoDBFilter NotEq(string field, object value)
		{
			QueryDocument doc = new QueryDocument(field, new BsonDocument("$ne", BsonValue.Create(value) ?? BsonNull.Value));
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 创建一个Like关系的过滤条件
		/// </summary>
		/// <param name="field"></param>
		/// <param name="regex"></param>
		/// <returns></returns>
		public static MongoDBFilter Lk(string field, Regex regex)
		{
			BsonRegularExpression mongoRegex = new BsonRegularExpression(regex);
			QueryDocument doc = new QueryDocument(field, mongoRegex);
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		/// <summary>
		/// 将Json字符串转换为MongoDBFilter对象
		/// </summary>
		/// <param name="json"></param>
		/// <returns></returns>
		public static MongoDBFilter Parse(string json)
		{
			QueryDocument doc = new QueryDocument(BsonDocument.Parse(json));
			MongoDBFilter mdbFilter = new MongoDBFilter(doc);
			return mdbFilter;
		}

		#endregion

		public override string ToString()
		{
			return QueryDoc.ToString();
		}
	}
}
