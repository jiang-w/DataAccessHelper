using BigData.Server.DataAccessHelper.Utility;
using ElasticSearch.Client.QueryDSL;

namespace BigData.Server.DataAccessHelper.ElasticSearch
{
	/// <summary>
	/// 排序
	/// 目前只支持单个字段排序
	/// </summary>
	public class ElasticSearchSort
	{
		internal SortItem Sort { get; private set; }

		public ElasticSearchSort(string fieldName, SortMode mode)
		{
			this.Sort = new SortItem(fieldName, mode == SortMode.ASC ? SortType.Asc : SortType.Desc);
		}
	}
}
