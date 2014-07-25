using System;
using System.Configuration;
using System.IO;

namespace BigData.Server.DataAccessHelper.Configuration
{
	static class ApplicationPath
	{
		/// <summary>
		/// 当前应用程序的根目录
		/// </summary>
		public static string AppRootDir
		{
			get { return AppDomain.CurrentDomain.BaseDirectory; }
		}

		/// <summary>
		/// 数据访问配置文件路径
		/// </summary>
		public static string BaseConfigFile
		{
			get
			{
				string configPath;
				if (!string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["DataAccessConfigFile"]))
					configPath = ConfigurationManager.AppSettings["DataAccessConfigFile"];
				else
					configPath = "Config\\DataAccess.config";
				return Path.Combine(AppRootDir, configPath);
			}
		}

		/// <summary>
		/// 缓存配置文件路径
		/// </summary>
		public static string CacheConfigFile
		{
			get
			{
				string configPath;
				if (!string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["DataCacheConfigFile"]))
					configPath = ConfigurationManager.AppSettings["DataCacheConfigFile"];
				else
					configPath = "Config\\DataCache.config";
				return Path.Combine(AppRootDir, configPath);
			}
		}

		/// <summary>
		/// 数据库访问驱动配置文件路径
		/// </summary>
		public static string ProviderConfigFile
		{
			get
			{
				return Path.Combine(AppRootDir, DataAccessConfig.Instance.DatabaseConfig.ProviderConfigPath);
			}
		}

		/// <summary>
		/// Sql语句配置文件所在目录
		/// </summary>
		public static string SqlMapDir
		{
			get
			{
				return Path.Combine(AppRootDir, DataAccessConfig.Instance.DatabaseConfig.SqlMapDirectory);
			}
		}
	}
}
