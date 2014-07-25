
namespace BigData.Server.DataAccessHelper.DataSource
{
	/// <summary>
	/// 数据源访问抽象工厂类
	/// </summary>
	abstract class DsAccesserFactory
	{
		public abstract DsAccesser Create(DataSourceObject dsObj);
	}

	/// <summary>
	/// 数据库数据源访问工厂类，单件模式
	/// </summary>
	class DsDatabaseAccesserFactory : DsAccesserFactory
	{
		static readonly DsDatabaseAccesserFactory _instance = new DsDatabaseAccesserFactory();

		public static DsDatabaseAccesserFactory Instance
		{
			get
			{
				return _instance;
			}
		}

		DsDatabaseAccesserFactory()
		{
		}

		public override DsAccesser Create(DataSourceObject dsObj)
		{
			return new DsDatabaseAccesser(dsObj);
		}
	}

	/// <summary>
	/// MongoDB数据源访问工厂类，单件模式
	/// </summary>
	class DsMongoAccesserFactory : DsAccesserFactory
	{
		static readonly DsMongoAccesserFactory _instance = new DsMongoAccesserFactory();

		public static DsMongoAccesserFactory Instance
		{
			get
			{
				return _instance;
			}
		}

		DsMongoAccesserFactory()
		{
		}

		public override DsAccesser Create(DataSourceObject dsObj)
		{
			return new DsMongoAccesser(dsObj);
		}
	}
}
