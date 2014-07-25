using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using BigData.Server.DataAccessHelper.MongoDb;
using BigData.Server.DataAccessHelper.Utility;

namespace BigData.Server.DataAccessHelper.DataSource
{
	public class DataSourceAccess
	{
		/// <summary>
		/// 数据源参数   参数值形式（字符串：'value'  日期：date'2012-01-20'  函数：FUNC_TRD_DT(2,2011-11-30,0)）
		/// </summary>
		private Dictionary<string, object> _paramDict;
		private DataSourceObject _dsObj;

		public string[] Fields { get; set; }
		public UQuery Filter { get; set; }
		public SortField[] Sort { get; set; }

		private DataSourceAccess(params KeyValuePair<string, object>[] dsParams)
		{
			_paramDict = dsParams.ToDictionary(param => param.Key.ToUpper(), param => param.Value, StringComparer.CurrentCultureIgnoreCase);
			this.Filter = _paramDict.ContainsKey("FILTER") ? UQuery.DeserializeFromJson(_paramDict["FILTER"].ToString()) : null;
			this.Sort = _paramDict.ContainsKey("ORDER") ? SortBuilder.DeserializeFromJson(_paramDict["ORDER"].ToString()) : null;
			this.Fields = _paramDict.ContainsKey("DISPLAYFIELDS") ? _paramDict["DISPLAYFIELDS"].ToString().Split(',')
				.Where(field => !string.IsNullOrWhiteSpace(field)).ToArray() : null;
		}

		public DataSourceAccess(string name, params KeyValuePair<string, object>[] dsParams)
			: this(dsParams)
		{
			_dsObj = new DataSourceObject(name);
		}

		public DataSourceAccess(long dsId, params KeyValuePair<string, object>[] dsParams)
			: this(dsParams)
		{
			_dsObj = new DataSourceObject(dsId);
		}

		public DataTable Execute()
		{
			DsAccesser dsAccess = GetDsAccesser();
			dsAccess.Fields = this.Fields;
			dsAccess.Filter = this.Filter;
			dsAccess.Sort = this.Sort;
			DataTable returnData = dsAccess.Execute();
			return returnData;
		}

		public DataTable Execute(int pageSize, int pageIndex, out int pageCount, out int recordCount)
		{
			DsAccesser dsAccess = GetDsAccesser();
			dsAccess.Fields = Fields;
			dsAccess.Filter = Filter;
			dsAccess.Sort = Sort;
			DataTable returnData = dsAccess.Execute(pageSize, pageIndex, out pageCount, out recordCount);
			return returnData;
		}

		private DsAccesser GetDsAccesser()
		{
			DsAccesserFactory accessFactory = null;
			_dsObj.FillInnerParameters(_paramDict);
			bool IsMongoData = false;
			MongoDBConnection conn = MongoDBConnection.Default;
			MongoDBAccess mongoAccess = new MongoDBAccess(conn);
			switch (_dsObj.Ds_Type) {
				case 2:
				case 3:
				case 7:
					conn.Database = "DS_STATUS";
					conn.Collection = "DS_STATUS";
					mongoAccess.Filter = MongoDBFilter.And(MongoDBFilter.Eq("DS_ID", _dsObj.Obj_ID), MongoDBFilter.Eq("DS_STAS", 0));
					IsMongoData = mongoAccess.Count() > 0;
					break;
				case 4:
					conn.Database = "TXT_RRP_STATUS";
					conn.Collection = "TXT_RRP_STATUS";
					mongoAccess.Filter = MongoDBFilter.And(MongoDBFilter.Eq("DS_NAME", _dsObj.Eng_Name), MongoDBFilter.Eq("DS_STAS", 0));
					IsMongoData = mongoAccess.Count() > 0;
					break;
			}

			if (IsMongoData) {
				accessFactory = DsMongoAccesserFactory.Instance;
			}
			else {
				accessFactory = DsDatabaseAccesserFactory.Instance;
			}
			DsAccesser accesser = accessFactory.Create(_dsObj);
			return accesser;
		}

		internal DataTable GetCacheRefreshData()
		{
			DsAccesser dsAccess = GetDsAccesser();
			return dsAccess.Execute();
		}
	}
}
