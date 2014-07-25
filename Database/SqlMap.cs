using System;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using System.Xml;
using BigData.Server.DataAccessHelper.Configuration;

namespace BigData.Server.DataAccessHelper.Database
{
	public class SqlMap
	{
		private readonly DatabaseConnection _conn;
		private readonly string _sqlMapPath;

		public SqlMap(DatabaseConnection connection, string sqlMapName)
		{
			_conn = connection;
			_sqlMapPath = Path.Combine(ApplicationPath.SqlMapDir, sqlMapName);
		}

		private string GenerateSql(string statementName)
		{
			string sql = string.Empty;
			XmlDocument xmlDoc = new XmlDocument();
			xmlDoc.Load(_sqlMapPath);
			XmlNode xmlNode = xmlDoc.SelectSingleNode(string.Format("/sqlMap/statements/select[@id=\"{0}\"]", statementName));
			if (xmlNode != null) {
				sql = xmlNode.InnerText.Trim();
				string parameterPrefix = _conn.Provider.ParameterPrefix;
				sql = Regex.Replace(sql, @"\${.+?}", item => {
					string matchVaue = item.Value;
					return parameterPrefix + matchVaue.Remove(matchVaue.Length - 1, 1).Remove(0, 2);
				});
			}
			else {
				throw new Exception(string.Format("在SqlMap文件中未找到名为\"{0}\"的节点 ({1})", statementName, _sqlMapPath));
			}
			return sql;
		}

		#region Public Method
		public int ExecuteNonQuery(string statementName, params DatabaseParameter[] dbParams)
		{
			try {
				string sql = GenerateSql(statementName);
				DatabaseAccess access = new DatabaseAccess(_conn);
				return access.ExecuteNonQuery(sql, dbParams);
			}
			catch (Exception ex) {
				throw new Exception(string.Format("Sql执行出错，节点名\"{0}\" ({1})", statementName, _sqlMapPath), ex);
			}
		}

		public object ExcuteScalar(string statementName, params DatabaseParameter[] dbParams)
		{
			try {
				string sql = GenerateSql(statementName);
				DatabaseAccess access = new DatabaseAccess(_conn);
				return access.ExcuteScalar(sql, dbParams);
			}
			catch (Exception ex) {
				throw new Exception(string.Format("Sql执行出错，节点名\"{0}\" ({1})", statementName, _sqlMapPath), ex);
			}
		}

		public DataSet ExecuteDataset(string statementName, params DatabaseParameter[] dbParams)
		{
			try {
				string sql = GenerateSql(statementName);
				DatabaseAccess access = new DatabaseAccess(_conn);
				return access.ExecuteDataset(sql, dbParams);
			}
			catch (Exception ex) {
				throw new Exception(string.Format("Sql执行出错，节点名\"{0}\" ({1})", statementName, _sqlMapPath), ex);
			}
		}

		public DataTable ExecuteDataTable(string statementName, params DatabaseParameter[] dbParams)
		{
			try {
				string sql = GenerateSql(statementName);
				DatabaseAccess access = new DatabaseAccess(_conn);
				return access.ExecuteDataTable(sql, dbParams);
			}
			catch (Exception ex) {
				throw new Exception(string.Format("Sql执行出错，节点名\"{0}\" ({1})", statementName, _sqlMapPath), ex);
			}
		}

		public DataTable ExecuteDataTable(string statementName, int pageSize, int pageIndex, out int pageCount, out int recordCount, params DatabaseParameter[] dbParams)
		{
			try {
				string sql = GenerateSql(statementName);
				DatabaseAccess access = new DatabaseAccess(_conn);
				return access.ExecuteDataTable(sql, pageSize, pageIndex, out  pageCount, out  recordCount, dbParams);
			}
			catch (Exception ex) {
				throw new Exception(string.Format("Sql执行出错，节点名\"{0}\" ({1})", statementName, _sqlMapPath), ex);
			}
		}

		public IDataReader ExecuteReader(string statementName, params DatabaseParameter[] dbParams)
		{
			try {
				string sql = GenerateSql(statementName);
				DatabaseAccess access = new DatabaseAccess(_conn);
				return access.ExecuteReader(sql, dbParams);
			}
			catch (Exception ex) {
				throw new Exception(string.Format("Sql执行出错，节点名\"{0}\" ({1})", statementName, _sqlMapPath), ex);
			}
		}

		public void ExecuteProcedure(string statementName, params DatabaseParameter[] dbParams)
		{
			try {
				string sql = GenerateSql(statementName);
				DatabaseAccess access = new DatabaseAccess(_conn);
				access.ExecuteProcedure(sql, dbParams);
			}
			catch (Exception ex) {
				throw new Exception(string.Format("Sql执行出错，节点名\"{0}\" ({1})", statementName, _sqlMapPath), ex);
			}
		}
		#endregion
	}
}
