using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using BigData.Server.DataAccessHelper.DataSource;
using BigData.Server.DataAccessHelper.MongoDb;
using MongoDB.Bson;
using MongoDB.Driver;

namespace BigData.Server.DataAccessHelper.Index
{
	class IdxMongoAccesser : IdxAccesser
	{
		/// <summary>
		/// MongoDB连接
		/// </summary>
		public MongoDBConnection Connection { get; set; }

		public IdxMongoAccesser(IndexObject idxObj)
		{
			this.Connection = MongoDBConnection.Index;
			base.IdxObj = idxObj;
		}

		/// <summary>
		/// 从Mongodb中获取指标数据
		/// </summary>
		/// <param name="idxParams">指标参数键值</param>
		/// <returns></returns>
		public override object Execute(IEnumerable<KeyValuePair<string, object>> idxParams)
		{
			string key = GenerateMongoKey(base.IdxObj, idxParams);
			MongoClient mongoClient = new MongoClient(Connection.ConnectionString);
			MongoServer mongoServer = mongoClient.GetServer();
			if (mongoServer.DatabaseExists(Connection.Database)
				&& mongoServer.GetDatabase(Connection.Database).CollectionExists(Connection.Collection)) {
				MongoDatabase mongoDb = mongoServer.GetDatabase(Connection.Database);
				MongoCollection mongoColl = mongoDb.GetCollection(Connection.Collection);
				IMongoQuery query = new QueryDocument("Key", BsonValue.Create(key));
				var selected = mongoColl.FindAs<BsonDocument>(query);
				BsonDocument doc = selected.FirstOrDefault();
				if (doc != null)
					return ConvertToSystemObject(doc.GetValue("Value"));
				else
					return null;
			}
			else {
				throw new Exception(string.Format("MongoDB找不到指定的集合{2}（服务器：{0}，数据库：{1}）"
				, Connection.ConnectionString, Connection.Database, Connection.Collection));
			}
		}

		#region 生成在Mongodb中访问指标数据的key
		private string GenerateMongoKey(IndexObject idx, IEnumerable<KeyValuePair<string, object>> idxParams)
		{
			var keyItems = idx.IDX_PARAM.OrderBy(p => p.Name).Select(p => {
				var temp = idxParams.Where(item => string.Equals(item.Key, p.Name
					, StringComparison.CurrentCultureIgnoreCase));
				if (temp.Count() > 0) {
					var kv = temp.First();
					string paramValue = kv.Value.ToString();
					DataSourceFunction dsFunc = new DataSourceFunction();
					if (dsFunc.IsFunction(paramValue)) {
						paramValue = dsFunc.ScalarFunction(paramValue);
					}
					return FormatParamValue(paramValue);
				}
				else {
					throw new Exception(string.Format("未指定指标{0}参数{1}", base.IdxObj.ENG_SHT, p.Name));
				}
			}).ToList();
			keyItems.Insert(0, base.IdxObj.IDX_ID.ToString());
			return string.Join("_", keyItems);
		}

		private string FormatParamValue(string value)
		{
			if (Regex.IsMatch(value, @"^date'\d{4}-\d{1,2}-\d{1,2}'$") ||
				Regex.IsMatch(value, @"\d{4}-\d{1,2}-\d{1,2}"))//日期型字符串date'2011-01-30'
            {
				string dateStr = Regex.Match(value, @"\d{4}-\d{1,2}-\d{1,2}").Value;
				return Convert.ToDateTime(dateStr).Date.ToString("yyyy-MM-dd HH:mm:ss");
			}
			else if (Regex.IsMatch(value, @"^'.*'$"))//字符串类型'abc'
            {
				return value.Substring(1, value.Length - 2);
			}
			else {
				return value;
			}
		}

		#endregion

		/// <summary>
		/// 类型转换
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		private object ConvertToSystemObject(BsonValue value)
		{
			switch (value.BsonType) {
				case BsonType.Boolean: return value.AsBoolean;
				case BsonType.DateTime: return value.AsLocalTime;
				case BsonType.Double: return value.AsDouble;
				case BsonType.Int32: return value.AsInt32;
				case BsonType.Int64: return value.AsInt64;
				case BsonType.String: return value.AsString;
				case BsonType.Null: return DBNull.Value;
				case BsonType.Document: return value.AsBsonDocument.ToString();
				case BsonType.Array: return value.AsBsonArray.ToString();
				default: return value.AsBsonDocument.ToString();
			}
		}
	}
}
