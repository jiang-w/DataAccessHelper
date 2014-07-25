using System;
using System.Linq;
using BigData.Server.DataAccessHelper.Configuration;

namespace BigData.Server.DataAccessHelper.MongoDb
{
	public class MongoDBConnection
	{
		#region Properties
		/// <summary>
		/// 连接字符串的Server对象，主机IP地址
		/// </summary>
		public string Host { get; set; }

		/// <summary>
		/// 连接字符串的Server对象，主机端口号
		/// </summary>
		public int Port { get; set; }

		/// <summary>
		/// 数据库
		/// </summary>
		public string Database { get; set; }

		/// <summary>
		/// 集合
		/// </summary>
		public string Collection { get; set; }

		/// <summary>
		/// 获取连接字符串
		/// </summary>
		public string ConnectionString
		{
			get { return GetConnectionString(); }
		}
		#endregion

		#region Constructor
		private MongoDBConnection()
		{
		}

		public MongoDBConnection(string hostIP, string database, string collection, int port = 27017)
		{
			if (string.IsNullOrWhiteSpace(hostIP))
				throw new ArgumentException("参数值不能为null、空或空白字符串", "hostIP");
			if (string.IsNullOrWhiteSpace(database))
				throw new ArgumentException("参数值不能为null、空或空白字符串", "database");
			if (string.IsNullOrWhiteSpace(collection))
				throw new ArgumentException("参数值不能为null、空或空白字符串", "collection");

			this.Host = hostIP;
			this.Port = port;
			this.Database = database;
			this.Collection = collection;
		}
		#endregion

		#region Generate ConnectionString
		private string GetConnectionString()
		{
			string connectionString = string.Empty;
			if (this.Port != 27017) {
				connectionString = string.Format(@"Server={0}:{1}", this.Host, this.Port);
			}
			else {
				connectionString = string.Format(@"Server={0}", this.Host);
			}
			return connectionString;
		}
		#endregion

		#region Static
		/// <summary>
		/// 根据配置文件生成DBConnection对象
		/// </summary>
		/// <param name="name">数据连接配置名称</param>
		/// <returns>返回DBConnection对象</returns>
		public static MongoDBConnection GetConnectionFromConfig(string name)
		{
			MongoDBConfig mongoConfig = DataAccessConfig.Instance.MongoDBConfig;
			MongoDBConnectionConfig connConfig = mongoConfig.Connections.Where(conn => conn.Name == name).LastOrDefault();
			if (connConfig != null) {
				MongoDBConnection conn = new MongoDBConnection() {
					Host = connConfig.Server,
					Port = connConfig.Port,
					Database = connConfig.Database,
					Collection = connConfig.Collection
				};
				return conn;
			}
			else {
				return null;
			}
		}

		/// <summary>
		/// 获取默认的MongoDB连接
		/// </summary>
		public static MongoDBConnection Default
		{
			get { return GetConnectionFromConfig("Default"); }
		}

		public static MongoDBConnection Index
		{
			get { return GetConnectionFromConfig("Index"); }
		}
		#endregion
	}
}
