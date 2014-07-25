
namespace BigData.Server.DataAccessHelper.Index
{
	abstract class IdxAccesserFactory
	{
		public abstract IdxAccesser Create(IndexObject dsObj);
	}

	class IdxMongoAccesserFactory : IdxAccesserFactory
	{
		public override IdxAccesser Create(IndexObject dsObj)
		{
			return new IdxMongoAccesser(dsObj);
		}
	}
}
