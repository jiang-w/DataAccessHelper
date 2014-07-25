using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using BigData.Server.DataAccessHelper.Database;
using Newtonsoft.Json;

namespace BigData.Server.DataAccessHelper.DataSource
{
	public class DataSourceObject
	{
		public long Obj_ID { get; set; }
		public string Eng_Name { get; set; }
		public string SQL_Clause { get; set; }
		public DsParameter[] InnerParams { get; set; }
		/// <summary>
		/// 1:动态  2：静态  3：静态分段  4：资讯类  7：增量更新
		/// </summary>
		public Int16 Ds_Type { get; set; }
		public string DbConn_Name { get; set; }

		public DataSourceObject(long dsId)
		{
			int db_typ;
			switch (DatabaseConnection.Default.DatabaseType) {
				case DatabaseType.Oracle: db_typ = 1;
					break;
				case DatabaseType.SQLServer: db_typ = 2;
					break;
				case DatabaseType.MySql: db_typ = 3;
					break;
				default: db_typ = 1;
					break;
			}
			SqlMap dataAccess = new SqlMap(DatabaseConnection.Default, "DataSourceSql.xml");
			DataView dsView = dataAccess.ExecuteDataTable("GetDataSourceById"
				, new DatabaseParameter("dbTyp", db_typ)
				, new DatabaseParameter("id", dsId)).DefaultView;
			if (dsView.Count > 0) {
				InitObj(dsView[0]);
			}
			else {
				throw new Exception(string.Format("未找到指定的数据源(OBJ_ID：{0})", dsId));
			}
		}

		public DataSourceObject(string dsName)
		{
			int db_typ;
			switch (DatabaseConnection.Default.DatabaseType) {
				case DatabaseType.Oracle: db_typ = 1;
					break;
				case DatabaseType.SQLServer: db_typ = 2;
					break;
				case DatabaseType.MySql: db_typ = 3;
					break;
				default: db_typ = 1;
					break;
			}
			SqlMap dataAccess = new SqlMap(DatabaseConnection.Default, "DataSourceSql.xml");
			DataView dsView = dataAccess.ExecuteDataTable("GetDataSourceByName"
				, new DatabaseParameter("dbTyp", db_typ)
				, new DatabaseParameter("name", dsName)).DefaultView;
			if (dsView.Count > 0) {
				InitObj(dsView[0]);
			}
			else {
				throw new Exception(string.Format("未找到指定的数据源(DS_ENG_NAME：{0})", dsName));
			}
		}

		private void InitObj(DataRowView drv)
		{
			this.Obj_ID = Convert.ToInt64(drv["OBJ_ID"]);
			this.Ds_Type = Convert.ToInt16(drv["DS_TYP"]);
			this.Eng_Name = drv["DS_ENG_NAME"].ToString();
			this.InnerParams = JsonConvert.DeserializeObject<DsParameter[]>(drv["PARAM_DFT_VAL"].ToString()) ?? new DsParameter[0];
			if (this.Ds_Type == 7)
				this.SQL_Clause = drv["INIT_SQL"].ToString();//2013-2-26  增量更新数据源取初始化SQL语句
			else
				this.SQL_Clause = drv["SQL_CLAUSE"].ToString();
			this.DbConn_Name = drv["DB_NAME"].ToString();
		}

		/// <summary>
		/// 内部参数赋值
		/// </summary>
		/// <param name="paramDic"></param>
		public void FillInnerParameters(Dictionary<string, object> paramDic)
		{
			InnerParams = InnerParams.Select(p => {
				if (Ds_Type == 7 && !paramDic.ContainsKey(p.Name)) {
					p.Value = null;//增量更新数据源可以不必传递数据源参数。为了兼容老视图配置，已经传递的则赋值，没有传递的设为null
				}
				else if (paramDic.ContainsKey(p.Name)) {
					p.Value = DsParameter.ParseValue(paramDic[p.Name].ToString());
				}
				else {
					throw new Exception(string.Format("未指定数据源{0}内部参数{1}", Eng_Name, p.Name));
				}
				return p;
			}).ToArray();
		}
	}
}
