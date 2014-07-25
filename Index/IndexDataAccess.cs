using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using BigData.Server.DataAccessHelper.MongoDb;
using MongoDB.Bson;
using MongoDB.Driver;

namespace BigData.Server.DataAccessHelper.Index
{
	public class IndexDataAccess
	{
		private IndexObject _idxObj;

		public IndexDataAccess(long idxId)
		{
			_idxObj = new IndexObject(idxId);
		}

		public object Execute(IEnumerable<KeyValuePair<string, object>> idxParams)
		{
			IdxAccesser idxAccess = GetIdxAccesser();
			return idxAccess.Execute(idxParams);
		}

		public static Dictionary<string, object> GetDataFromMongoByKeys(params string[] keys)
		{
			Dictionary<string, object> idxDic = new Dictionary<string, object>();
			MongoDBConnection conn = MongoDBConnection.Index;
			MongoClient mongoClient = new MongoClient(conn.ConnectionString);
			MongoServer mongoServer = mongoClient.GetServer();
			if (mongoServer.DatabaseExists(conn.Database)
				&& mongoServer.GetDatabase(conn.Database).CollectionExists(conn.Collection)) {
				MongoDatabase mongoDb = mongoServer.GetDatabase(conn.Database);
				MongoCollection mongoColl = mongoDb.GetCollection(conn.Collection);
				IMongoQuery query = new QueryDocument("Key", new BsonDocument("$in", new BsonArray(keys)));
				var selected = mongoColl.FindAs<BsonDocument>(query);
				var cursor = selected.GetEnumerator();
				while (cursor.MoveNext()) {
					BsonDocument doc = cursor.Current;
					idxDic[doc.GetValue("Key").AsString] = ConvertToSystemObject(doc.GetValue("Value"));
				}
				return idxDic;
			}
			else {
				throw new Exception(string.Format("MongoDB找不到指定的集合{2}（服务器：{0}，数据库：{1}）"
				, conn.ConnectionString, conn.Database, conn.Collection));
			}
		}

		private IdxAccesser GetIdxAccesser()
		{
			IdxAccesserFactory accessFactory = null;
			accessFactory = new IdxMongoAccesserFactory();
			IdxAccesser accesser = accessFactory.Create(_idxObj);
			return accesser;
		}

		/// <summary>
		/// 类型转换
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		private static object ConvertToSystemObject(BsonValue value)
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
