using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using ElasticSearch.Client;
using ElasticSearch.Client.Exception;
using ElasticSearch.Client.QueryDSL;

namespace BigData.Server.DataAccessHelper.ElasticSearch
{
	public class ElasticSearchAccess
	{
		public string ClusterName { get; private set; }

		public string IndexName { get; private set; }

		public string TypeName { get; private set; }

		public ElasticSearchFields Fields { get; set; }

		public ElasticSearchSort Sort { get; set; }

		public ElasticSearchFilter Filter { get; set; }

		public ElasticSearchAccess(string cluster, string index, string type)
		{
			this.ClusterName = cluster;
			this.IndexName = index;
			this.TypeName = type;
		}

		public DataTable Execute(int pageSize, int pageIndex, out int pageCount, out int recordCount)
		{
			ElasticSearchClient client = new ElasticSearchClient(this.ClusterName);
			IQuery query = (Filter == null) ? new MatchAllQuery() : Filter.Query;
			string[] fields = (Fields != null && Fields.Fields.Count() > 0) ? Fields.Fields.ToArray() : null;
			SortItem sortItem = (Sort != null) ? Sort.Sort : null;
			int from = (pageIndex - 1) * pageSize;

			var searchResult = client.Search(this.IndexName, this.TypeName, query, sortItem, from, pageSize, fields);
			recordCount = searchResult.GetTotalCount();
			pageCount = Convert.ToInt32(Math.Ceiling(recordCount * 1.0 / pageSize));
			DataTable table = FillDataTable(searchResult.GetHits().Hits.Select(item => fields == null || fields.Count() == 0 ? item.Source : item.Fields));
			return table;
		}

		private DataTable CreateTableSchema()
		{
			DataTable table = new DataTable(this.TypeName);
			ElasticSearchClient client = new ElasticSearchClient(this.ClusterName);
			string typeName = this.TypeName.Substring(0, this.TypeName.Length - 6) + "_prop";
			var searchResult = client.Search(this.IndexName, typeName, new MatchAllQuery(), null, 0, 1000, new string[] { "PROP_NAME", "PROP_TYPE" });
			var propList = searchResult.GetHits().Hits.Select(item => item.Fields)
			   .Select(dic => new { PROP_NAME = dic["PROP_NAME"].ToString(), PROP_TYPE = dic["PROP_TYPE"].ToString() });
			if (propList.Count() == 0) {
				throw new ElasticSearchException(string.Format("ElasticSearch访问出错(IndexName:{0},TypeName:{1})：缺少数据"
					, this.IndexName, typeName));
			}

			if (this.Fields != null && this.Fields.Fields.Count() > 0) {
				propList = propList.Where(item => this.Fields.Fields.Contains(item.PROP_NAME));
			}
			propList.ToList().ForEach(item => {
				switch (item.PROP_TYPE) {
					case "String":
						table.Columns.Add(item.PROP_NAME, typeof(string));
						break;
					case "Decimal":
						table.Columns.Add(item.PROP_NAME, typeof(decimal));
						break;
					case "DateTime":
						table.Columns.Add(item.PROP_NAME, typeof(DateTime));
						break;
					default:
						table.Columns.Add(item.PROP_NAME, typeof(string));
						break;
				}
			});
			return table;
		}

		private DataTable FillDataTable(IEnumerable<Dictionary<string, object>> dataList)
		{
			DataTable table = CreateTableSchema();
			var cursor = dataList.GetEnumerator();
			while (cursor.MoveNext()) {
				DataRow row = table.NewRow();
				var data = cursor.Current;
				foreach (string key in data.Keys) {
					row[key] = data[key] ?? DBNull.Value;
				}
				table.Rows.Add(row);
			}
			return table;
		}
	}
}
