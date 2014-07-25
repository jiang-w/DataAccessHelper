using System.Data;
using BigData.Server.DataAccessHelper.Database;
using BigData.Server.DataAccessHelper.Utility;

namespace BigData.Server.DataAccessHelper.DataSource
{
	abstract class DsAccesser
	{
		public DataSourceObject DsObj { get; set; }
		public string[] Fields { get; set; }
		public UQuery Filter { get; set; }
		public SortField[] Sort { get; set; }

		public abstract DataTable Execute();

		public abstract DataTable Execute(int pageSize, int pageIndex, out int pageCount, out int recordCount);
	}
}
