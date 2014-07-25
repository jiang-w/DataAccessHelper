using System;
using System.Collections.Generic;
using System.Data;
using BigData.Server.DataAccessHelper.Database;

namespace BigData.Server.DataAccessHelper.DataSource
{
	class DsDbConnConfig
	{
		private readonly static Dictionary<string, DatabaseConnection> _dic = new Dictionary<string, DatabaseConnection>();

		static DsDbConnConfig()
		{
			DatabaseAccess dbAccess = new DatabaseAccess(DatabaseConnection.Default);
			using (DataTable dt = dbAccess.ExecuteDataTable("SELECT * FROM dsk_db_cfg")) {
				if (dt != null) {
					foreach (DataRow dr in dt.Rows) {
						string name = dr["DB_NAME"].ToString();
						string ip = dr["DB_IP"].ToString();
						string database = dr["SID"].ToString();
						int port = Convert.ToInt32(dr["DB_PORT"]);
						string userId = dr["USR_NAME"].ToString();
						string pwd = dr["USR_PWD"].ToString();
						DatabaseType typ;
						switch (dr["DB_TYP"].ToString()) {
							case "1":
								typ = DatabaseType.Oracle;
								break;
							case "2":
								typ = DatabaseType.SQLServer;
								break;
							case "3":
								typ = DatabaseType.MySql;
								break;
							default:
								typ = DatabaseType.Oracle;
								break;
						}
						DatabaseConnection conn = new DatabaseConnection(ip, port, database, userId, pwd, typ);
						_dic[name] = conn;
					}
				}
			}
		}

		public static DatabaseConnection GetDbConnectionByName(string name)
		{
			if (!string.IsNullOrEmpty(name) && _dic.ContainsKey(name)) {
				return _dic[name];
			}
			else {
				return DatabaseConnection.Default;
			}
		}
	}
}
