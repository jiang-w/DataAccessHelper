using System.Collections.Generic;

namespace BigData.Server.DataAccessHelper.DataEntity
{
	class DbTableInfo
	{
		public string Name { get; set; }

		private readonly List<DbFieldInfo> _fields = new List<DbFieldInfo>();
		public List<DbFieldInfo> Fields
		{
			get { return _fields; }
		}
	}
}
