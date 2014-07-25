using System;
using System.Data;
using System.Linq;
using BigData.Server.DataAccessHelper.Database;
using BigData.Server.DataAccessHelper.Utility;

namespace BigData.Server.DataAccessHelper.DataSource
{
	class DsDatabaseAccesser : DsAccesser
	{
		public DatabaseConnection Connection { get; set; }

		public string SqlClause { get { return GetExeDsSql(); } }

		public DsDatabaseAccesser(DataSourceObject dsObj)
		{
			this.Connection = DsDbConnConfig.GetDbConnectionByName(dsObj.DbConn_Name);
			base.DsObj = dsObj;
		}

		public override DataTable Execute()
		{
			DataTable returnData = null;
			if (!string.IsNullOrEmpty(this.SqlClause)) {
				DatabaseAccess dbAccess = new DatabaseAccess(this.Connection);
				returnData = dbAccess.ExecuteDataTable(this.SqlClause);
				if (returnData != null)
					returnData.TableName = base.DsObj.Eng_Name;
			}
			return returnData;
		}

		public override DataTable Execute(int pageSize, int pageIndex, out int pageCount, out int recordCount)
		{
			DataTable returnData = null;
			pageCount = 0;
			recordCount = 0;
			if (!string.IsNullOrEmpty(this.SqlClause)) {
				DatabaseAccess dbAccess = new DatabaseAccess(this.Connection);
				returnData = dbAccess.ExecuteDataTable(this.SqlClause, pageSize, pageIndex, out pageCount, out recordCount);
				if (returnData != null)
					returnData.TableName = base.DsObj.Eng_Name;
			}
			return returnData;
		}

		#region 获得可执行SQL

		private string GetExeDsSql()
		{
			string sqlClause = string.Empty;
			if (!string.IsNullOrEmpty(base.DsObj.SQL_Clause)) {
				sqlClause = FillSql(base.DsObj.SQL_Clause, base.DsObj.InnerParams);
				SqlBuilder sqlBuilder = new SqlBuilder(sqlClause, this.Connection.DatabaseType);
				if (base.DsObj.Ds_Type == 7)//2013-2-27修改   增量更新数据源，将传递的数据源参数作为过滤参数处理
                {
					base.Filter = UQuery.And(base.DsObj.InnerParams.Where(p => p.Value != null).Select(p =>
						p.Value is Array ? UQuery.In(p.Name, (Array)p.Value) : UQuery.Eq(p.Name, p.Value)).ToArray());
				}
				sqlBuilder.Filter = base.Filter;
				sqlBuilder.Sort = base.Sort;
				sqlBuilder.Fields = new SqlFields().Add(base.Fields);
				sqlClause = sqlBuilder.Build();
			}
			return sqlClause;
		}

		private string FillSql(string sqlClause, DsParameter[] innerParams)
		{
			foreach (DsParameter param in innerParams) {
				if (param.Value != null) {
					string paramString = string.Empty;
					if (param.Value is Array)
						paramString = string.Join(",", ((object[])param.Value)
							.Select(item => ParamValueFormat(item)));
					else
						paramString = ParamValueFormat(param.Value);
					sqlClause = sqlClause.Replace("${" + param.Name + "}", paramString);
				}
			}
			return sqlClause;
		}

		private string ParamValueFormat(object paramValue)
		{
			string result;
			if (paramValue is DateTime)
				result = string.Format("date'{0}'", ((DateTime)paramValue).ToString("yyyy-MM-dd"));
			else if (paramValue is string)
				result = string.Format("'{0}'", paramValue);
			else
				result = paramValue.ToString();
			return result;
		}

		#endregion
	}
}
