using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace BigData.Server.DataAccessHelper.MongoDb
{
	public class MongoDBAccess
	{
		public MongoDBConnection Connection { get; set; }
		public MongoDBFilter Filter { get; set; }
		public MongoDBSort Sort { get; set; }
		public MongoDBFields Fields { get; set; }

		public MongoDBAccess(MongoDBConnection conn)
		{
			this.Connection = conn;
		}

		public MongoDBAccess(string hostIP, int port, string database, string collection)
		{
			MongoDBConnection conn = new MongoDBConnection(hostIP, database, collection, port);
			this.Connection = conn;
		}

		public MongoDBAccess(string hostIP, string database, string collection)
		{
			MongoDBConnection conn = new MongoDBConnection(hostIP, database, collection);
			this.Connection = conn;
		}

		public int Count()
		{
			int recordCount = 0;
			if (CollectionExists()) {
				MongoClient mongoClient = new MongoClient(this.Connection.ConnectionString);
				MongoServer mongoServer = mongoClient.GetServer();
				MongoDatabase mongoDb = mongoServer.GetDatabase(this.Connection.Database);
				MongoCollection mongoColl = mongoDb.GetCollection(this.Connection.Collection);
				QueryDocument filterDoc = Filter != null ? Filter.QueryDoc : null;
				recordCount = Convert.ToInt32(mongoColl.Count(filterDoc));
			}
			return recordCount;
		}

		public bool DatabaseExists()
		{
			if (string.IsNullOrWhiteSpace(this.Connection.Database)) {
				return false;
			}
			else {
				MongoClient mongoClient = new MongoClient(this.Connection.ConnectionString);
				MongoServer mongoServer = mongoClient.GetServer();
				return mongoServer.DatabaseExists(this.Connection.Database);
			}
		}

		public bool CollectionExists()
		{
			if (string.IsNullOrWhiteSpace(this.Connection.Database)
				|| string.IsNullOrWhiteSpace(this.Connection.Collection)) {
				return false;
			}
			else {
				MongoClient mongoClient = new MongoClient(this.Connection.ConnectionString);
				MongoServer mongoServer = mongoClient.GetServer();
				return mongoServer.DatabaseExists(this.Connection.Database)
					&& mongoServer.GetDatabase(this.Connection.Database).CollectionExists(this.Connection.Collection);
			}
		}

		public DataTable Execute()
		{
			DataTable resultTable = null;
			if (!CollectionExists()) {
				throw new Exception(string.Format("MongoDB找不到指定的集合{2}（服务器：{0}，数据库：{1}）"
				, this.Connection.ConnectionString, this.Connection.Database, this.Connection.Collection));
			}
			MongoClient mongoClient = new MongoClient(this.Connection.ConnectionString);
			MongoServer mongoServer = mongoClient.GetServer();
			MongoDatabase mongoDb = mongoServer.GetDatabase(this.Connection.Database);
			MongoCollection mongoColl = mongoDb.GetCollection(this.Connection.Collection);
			IMongoQuery query = Filter != null ? Filter.QueryDoc : null;
			var selected = mongoColl.FindAs<BsonDocument>(query);

			if (Fields != null) {
				selected.SetFields(Fields.Builder);
			}
			if (Sort != null) {
				selected.SetSortOrder(Sort.Builder);
			}
			resultTable = FillDataTable(selected);
			return resultTable;
		}

		public DataTable Execute(int pageSize, int pageIndex, out int pageCount, out int recordCount)
		{
			DataTable resultTable = null;
			recordCount = 0;
			pageCount = 0;
			if (!CollectionExists()) {
				throw new Exception(string.Format("MongoDB找不到指定的集合{2}（服务器：{0}，数据库：{1}）"
				, this.Connection.ConnectionString, this.Connection.Database, this.Connection.Collection));
			}
			MongoClient mongoClient = new MongoClient(this.Connection.ConnectionString);
			MongoServer mongoServer = mongoClient.GetServer();
			MongoDatabase mongoDb = mongoServer.GetDatabase(this.Connection.Database);
			MongoCollection mongoColl = mongoDb.GetCollection(this.Connection.Collection);
			IMongoQuery query = Filter != null ? Filter.QueryDoc : null;

			recordCount = Convert.ToInt32(mongoColl.Count(query));
			pageSize = pageSize > recordCount || pageSize <= 0 ? recordCount : pageSize;
			pageCount = pageSize != 0 ? Convert.ToInt32(Math.Ceiling(recordCount * 1.0 / pageSize)) : 0;
			pageIndex = pageIndex <= 0 ? 1 : pageIndex;
			pageIndex = pageIndex > pageCount ? pageCount : pageIndex;
			int skipCount = pageIndex > 0 ? (pageIndex - 1) * pageSize : 0;

			var selected = mongoColl.FindAs<BsonDocument>(query).SetSkip(skipCount).SetLimit(pageSize);
			if (Fields != null) {
				selected.SetFields(Fields.Builder);
			}
			if (Sort != null) {
				selected.SetSortOrder(Sort.Builder);
			}
			resultTable = FillDataTable(selected);
			return resultTable;
		}

		#region 创建DataTable并填充数据
		/// <summary>
		/// 创建要返回的表结构
		/// </summary>
		/// <param name="doc"></param>
		/// <returns></returns>
		private DataTable CreateTableSchema(BsonDocument doc)
		{
			DataTable table = null;
			string propCollection = "PROP";
			string propDatabase = this.Connection.Database;
			table = new DataTable(propDatabase);

			Dictionary<string, Type> fieldTypMapping = null;

			MongoClient mongoClient = new MongoClient(this.Connection.ConnectionString);
			MongoServer mongoServer = mongoClient.GetServer();
			MongoDatabase mongoDb = mongoServer.GetDatabase(propDatabase);
			MongoCollection mongoColl = mongoDb.GetCollection(propCollection);
			if (mongoColl.Count() == 0) {
				throw new Exception(string.Format("MongoDB找不到指定的集合{2}（服务器：{0}，数据库：{1}）"
				, this.Connection.ConnectionString, propDatabase, propCollection));
			}

			var selected = mongoColl.FindAllAs<BsonDocument>();
			fieldTypMapping = selected.ToDictionary(item => item["PROP_NAME"].ToString()
					, item => {
						switch (item["PROP_TYPE"].ToString()) {
							case "String": return typeof(string);
							case "DateTime": return typeof(DateTime);
							case "Decimal": return typeof(decimal);
							case "StringArray":
							case "DateTimeArray":
							case "DecimalArray":
							case "IntArray":
							case "LongArray":
							case "ObjArray":
							default: return typeof(string);
						}
					});

			if (doc != null) {
				foreach (var k in doc.Elements.Select(item => item.Name)) {
					if (fieldTypMapping.ContainsKey(k)) {
						table.Columns.Add(k, fieldTypMapping[k]);
					}
				}
			}
			else {
				foreach (KeyValuePair<string, Type> field in fieldTypMapping) {
					if (Fields != null) {
						if (Fields.DisplayMode == FieldDisplayMode.Hidden
							&& Fields.Builder.ToBsonDocument().Contains(field.Key)) {
							continue;
						}

						if (Fields.DisplayMode == FieldDisplayMode.Display
							&& !Fields.Builder.ToBsonDocument().Contains(field.Key)) {
							continue;
						}
					}
					table.Columns.Add(field.Key, field.Value);
				}
			}
			return table;
		}

		/// <summary>
		/// 填充表数据
		/// </summary>
		/// <param name="docs"></param>
		/// <returns></returns>
		private DataTable FillDataTable(IEnumerable<BsonDocument> docs)
		{
			DataTable table = CreateTableSchema(docs.FirstOrDefault());
			var cursor = docs.GetEnumerator();
			table.BeginLoadData();
			while (cursor.MoveNext()) {
				BsonDocument doc = cursor.Current;
				doc.Remove("_id");
				table.LoadDataRow(doc.Values.Select(item => ConvertToSystemObject(item)).ToArray(), true);
			}
			table.EndLoadData();
			return table;
		}

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
		#endregion
	}
}
