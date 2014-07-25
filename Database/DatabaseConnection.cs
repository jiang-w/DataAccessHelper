using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Xml;
using BigData.Server.DataAccessHelper.Configuration;

namespace BigData.Server.DataAccessHelper.Database
{
	public class DatabaseConnection
	{
		#region Fields & Properties
		/// <summary>
		/// 提供程序IDbProvider类型
		/// </summary>
		internal IDbProvider Provider { get; private set; }

		/// <summary>
		/// 提供程序名称
		/// </summary>
		public string ProviderName
		{
			get { return this.Provider.Name; }
		}

		/// <summary>
		/// 数据库类型
		/// </summary>
		public DatabaseType DatabaseType { get; private set; }

		/// <summary>
		/// 获取连接字符串
		/// </summary>
		public string ConnectionString { get; private set; }

		#endregion

		#region Constructor
		public DatabaseConnection(string connectionString, DatabaseType dbType, string provider)
		{
			this.Provider = GetProviderFromConfig(provider);
			this.ConnectionString = connectionString;
			this.DatabaseType = dbType;
		}

		public DatabaseConnection(string host, int port, string database, string userid, string password
			, DatabaseType dbType, string provider)
			: this(null, dbType, provider)
		{
			this.ConnectionString = GetConnectionString(host, port, database, userid, password, dbType);
		}

		public DatabaseConnection(string host, string database, string userid, string password
			, DatabaseType dbType, string provider)
			: this(null, dbType, provider)
		{
			this.ConnectionString = GetConnectionString(host, null, database, userid, password, dbType);
		}

		public DatabaseConnection(string connectionString, DatabaseType dbType)
		{
			switch (dbType) {
				case DatabaseType.Oracle: this.Provider = DbProvider.DefaultOracleProvider;
					break;
				case DatabaseType.SQLServer: this.Provider = DbProvider.DefaultSqlServerProvider;
					break;
				case DatabaseType.MySql: this.Provider = DbProvider.DefaultMySqlProvider;
					break;
			}
			this.ConnectionString = connectionString;
			this.DatabaseType = dbType;
		}

		public DatabaseConnection(string host, int port, string database, string userid, string password
			, DatabaseType dbType)
			: this(null, dbType)
		{
			this.ConnectionString = GetConnectionString(host, port, database, userid, password, dbType);
		}

		public DatabaseConnection(string host, string database, string userid, string password
			, DatabaseType dbType)
			: this(null, dbType)
		{
			this.ConnectionString = GetConnectionString(host, null, database, userid, password, dbType);
		}
		#endregion

		#region Static
		/// <summary>
		/// 获取默认的数据库连接
		/// </summary>
		public static DatabaseConnection Default
		{
			get { return GetConnectionFromConfig("Default"); }
		}

		/// <summary>
		/// 根据配置文件生成DBConnection对象
		/// </summary>
		/// <param name="name">数据连接配置名称</param>
		/// <returns>返回DBConnection对象</returns>
		public static DatabaseConnection GetConnectionFromConfig(string name)
		{
			DatabaseConfig dbConfig = DataAccessConfig.Instance.DatabaseConfig;
			DatabaseConnectionConfig connConfig = dbConfig.Connections.Where(conn => conn.Name == name).LastOrDefault();
			if (connConfig != null) {
				return new DatabaseConnection(connConfig.ConnectionString, connConfig.DatabaseType, connConfig.ProvideName);
			}
			else {
				return null;
			}
		}

		private static IDbProvider GetProviderFromConfig(string providerName)
		{
			IDbProvider provider;
			string providerConfigPath = ApplicationPath.ProviderConfigFile;
			if (!File.Exists(providerConfigPath)) {
				throw new FileNotFoundException("没有找到配置文件", providerConfigPath);
			}

			XmlDocument xmlDoc = new XmlDocument();
			xmlDoc.Load(providerConfigPath);
			var xmlnsm = new XmlNamespaceManager(xmlDoc.NameTable);
			xmlnsm.AddNamespace("ns", "http://ibatis.apache.org/providers");
			XmlNode xmlNode = xmlDoc.SelectSingleNode(string.Format("/ns:providers/ns:provider[@name=\"{0}\"]", providerName), xmlnsm);
			if (xmlNode != null)
				provider = ProviderDeserializer.Deserialize(xmlNode);
			else
				provider = null;

			if (provider != null && provider.IsEnabled) {
				provider.Initialize();
			}
			else {
				throw new Exception(string.Format("\"{0}\" does not exist or is not available.({1})", providerName, providerConfigPath));
			}

			return provider;
		}

		private static string GetConnectionString(string host, int? port, string database, string userid, string password, DatabaseType dbType)
		{
			string connectionString = string.Empty;
			switch (dbType) {
				case DatabaseType.Oracle:
					if (port != null && port != 1521)
						connectionString = string.Format(@"Data Source={0}:{1}/{2};User Id={3};Password={4};"
							, host, port, database, userid, password);
					else
						connectionString = string.Format(@"Data Source={0}/{1};User Id={2};Password={3};"
							, host, database, userid, password);
					break;
				case DatabaseType.SQLServer:
					if (port != null && port != 1433)
						connectionString = string.Format(@"Data Source={0},{1};Database={2};User ID={3};Password={4}"
							, host, port, database, userid, password);
					else
						connectionString = string.Format(@"Data Source={0};Database={1};User ID={2};Password={3}"
							, host, database, userid, password);
					break;
				case DatabaseType.MySql:
					if (port != null && port != 3306)
						connectionString = string.Format(@"Server={0}:{1};Database={2};User Id={3};Password={4}"
							, host, port, database, userid, password);
					else
						connectionString = string.Format(@"Server={0};Database={1};User Id={2};Password={3}"
							, host, database, userid, password);
					break;
			}
			return connectionString;
		}
		#endregion

		/// <summary>
		/// 测试数据连接是否能连接数据库
		/// </summary>
		/// <returns></returns>
		public bool Test()
		{
			bool result = false;
			IDbConnection conn = null;
			try {
				conn = this.Provider.CreateConnection();
				conn.ConnectionString = this.ConnectionString;
				conn.Open();
				if (conn.State == ConnectionState.Open)
					result = true;
			}
			finally {
				if (conn != null)
					conn.Dispose();
			}
			return result;
		}
	}
}
