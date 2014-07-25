using System;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using BigData.Server.DataAccessHelper.Database;

namespace BigData.Server.DataAccessHelper.Configuration
{
	sealed class DataAccessConfig
	{
		#region static
		private static DataAccessConfig instance;
		private static ConfigWatcher configWatcher;

		public static DataAccessConfig Instance
		{
			get { return instance; }
		}

		static DataAccessConfig()
		{
			string filePath = ApplicationPath.BaseConfigFile;
			if (File.Exists(filePath)) {
				instance = LoadConfig(filePath);
				WatchConfig(filePath);
			}
			else {
				throw new FileNotFoundException("没有找到配置文件", filePath);
			}
		}

		private static DataAccessConfig LoadConfig(string xmlPath)
		{
			try {
				var xDoc = XElement.Load(xmlPath);
				DataAccessConfig config = new DataAccessConfig {
					DatabaseConfig = new DatabaseConfig {
						ProviderConfigPath = xDoc.Element("databaseConfig").Element("providers").Value,
						SqlMapDirectory = xDoc.Element("databaseConfig").Element("sqlMapDirectory").Value,
						Connections = xDoc.Element("databaseConfig").Element("connections").Elements()
						.Select(e => new DatabaseConnectionConfig {
							Name = e.Attribute("name").Value,
							ConnectionString = e.Attribute("connectionString").Value,
							DatabaseType = (DatabaseType)Enum.Parse(typeof(DatabaseType), e.Attribute("dbType").Value, true),
							ProvideName = e.Attribute("provide").Value
						}).ToArray()
					},
					MongoDBConfig = new MongoDBConfig {
						Connections = xDoc.Element("mongoDBConfig").Element("connections").Elements()
						.Select(e => new MongoDBConnectionConfig {
							Name = e.Attribute("name").Value,
							Server = e.Attribute("server").Value,
							Port = int.Parse(e.Attribute("port").Value),
							Database = e.Attribute("database") != null ? e.Attribute("database").Value : null,
							Collection = e.Attribute("collection") != null ? e.Attribute("collection").Value : null
						}).ToArray()
					}
				};
				return config;
			}
			catch {
				throw new Exception(string.Format("读取数据访问配置出错（{0}）", xmlPath));
			}
		}

		private static void WatchConfig(string configPath)
		{
			FileInfo configFile = new FileInfo(configPath);
			configWatcher = new ConfigWatcher(configFile.Directory.FullName, configFile.Name);
			configWatcher.NotifyFilter = NotifyFilters.CreationTime | NotifyFilters.LastWrite;
			configWatcher.Changed += new FileSystemEventHandler(OnConfigFileChanged);
			configWatcher.Start();
		}

		private static void OnConfigFileChanged(object sender, FileSystemEventArgs e)
		{
			lock (instance) {
				instance = LoadConfig(e.FullPath);
			}
		}
		#endregion

		public DatabaseConfig DatabaseConfig { get; set; }

		public MongoDBConfig MongoDBConfig { get; set; }

		private DataAccessConfig()
		{
		}
	}

	#region DatabaseConfig
	class DatabaseConfig
	{
		public string ProviderConfigPath { get; set; }

		public string SqlMapDirectory { get; set; }

		public DatabaseConnectionConfig[] Connections { get; set; }
	}

	class DatabaseConnectionConfig
	{
		public string Name { get; set; }

		public string ConnectionString { get; set; }

		public DatabaseType DatabaseType { get; set; }

		public string ProvideName { get; set; }
	}
	#endregion

	#region MongoDBConfig
	class MongoDBConfig
	{
		public MongoDBConnectionConfig[] Connections { get; set; }
	}

	class MongoDBConnectionConfig
	{
		public string Name { get; set; }

		public string Server { get; set; }

		public int Port { get; set; }

		public string Database { get; set; }

		public string Collection { get; set; }
	}
	#endregion
}
