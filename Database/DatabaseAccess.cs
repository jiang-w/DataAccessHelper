using System;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;

namespace BigData.Server.DataAccessHelper.Database
{
	public class DatabaseAccess
	{
		readonly IDbProvider _provider;
		readonly IDbConnection _conn;
		readonly DatabaseType _dbType;

		#region Constructor
		public DatabaseAccess(DatabaseConnection connection)
		{
			if (connection == null) {
				throw new ArgumentNullException("connection");
			}
			else {
				_provider = connection.Provider;
				_conn = _provider.CreateConnection();
				_conn.ConnectionString = connection.ConnectionString;
				_dbType = connection.DatabaseType;
			}
		}
		#endregion

		#region ExecuteNonQuery
		/// <summary>
		/// 执行指定的SQL语句，并返回受影响行数
		/// </summary>
		/// <param name="sql">sql语句</param>
		/// <param name="dbParams">参数</param>
		/// <returns>返回受影响行数</returns>
		public int ExecuteNonQuery(string sql, params DatabaseParameter[] dbParams)
		{
			int effectRows = 0;
			using (IDbCommand cmd = CreateCommand(sql, CommandType.Text, dbParams)) {
				try {
					if (_conn.State == ConnectionState.Closed) {
						_conn.Open();
					}
				}
				catch (Exception ex) {
					string error = string.Format("数据库连接失败：连接字符串为（{0}）,{1}"
						, _conn.ConnectionString, ex.Message);
					throw new Exception(error, ex);
				}

				try {
					effectRows = cmd.ExecuteNonQuery();
				}
				catch (Exception ex) {
					string error = string.Format("SQL语句执行失败：（{0}）,{1}", cmd.CommandText, ex.Message);
					throw new Exception(error, ex);
				}
				finally {
					_conn.Close();
				}
			}
			return effectRows;
		}
		#endregion

		#region ExcuteScalar
		/// <summary>
		/// 执行指定的SQL语句，返回首行首列值
		/// </summary>
		/// <param name="sql">sql语句</param>
		/// <param name="dbParams">参数</param>
		/// <returns>返回首行首列值</returns>
		public object ExcuteScalar(string sql, params DatabaseParameter[] dbParams)
		{
			object returnval = null;
			using (IDbCommand cmd = CreateCommand(sql, CommandType.Text, dbParams)) {
				try {
					if (_conn.State == ConnectionState.Closed) {
						_conn.Open();
					}
				}
				catch (Exception ex) {
					string error = string.Format("数据库连接失败：连接字符串为（{0}）,{1}"
						, _conn.ConnectionString, ex.Message);
					throw new Exception(error, ex);
				}

				try {
					returnval = cmd.ExecuteScalar();
				}
				catch (Exception ex) {
					string error = string.Format("SQL语句执行失败：（{0}）,{1}", cmd.CommandText, ex.Message);
					throw new Exception(error, ex);
				}
				finally {
					_conn.Close();
				}
			}
			return returnval;
		}

		#endregion

		#region ExecuteDataset
		/// <summary>
		/// 执行指定的SQL语句，返回DataSet
		/// </summary>
		/// <param name="sql">sql语句</param>
		/// <param name="dbParams">参数</param>
		/// <returns>DataSet</returns>
		public DataSet ExecuteDataset(string sql, params DatabaseParameter[] dbParams)
		{
			DataSet ds = new DataSet();
			using (IDbCommand cmd = CreateCommand(sql, CommandType.Text, dbParams)) {
				try {
					if (_conn.State == ConnectionState.Closed) {
						_conn.Open();
					}
				}
				catch (Exception ex) {
					string error = string.Format("数据库连接失败：连接字符串为（{0}）,{1}"
						, _conn.ConnectionString, ex.Message);
					throw new Exception(error, ex);
				}

				try {
					IDbDataAdapter da = _provider.CreateDataAdapter();
					da.SelectCommand = cmd;
					da.Fill(ds);
				}
				catch (Exception ex) {
					string error = string.Format("SQL语句执行失败：（{0}）,{1}", cmd.CommandText, ex.Message);
					throw new Exception(error, ex);
				}
				finally {
					_conn.Close();
				}
			}
			return ds;
		}

		#endregion

		#region ExecuteDataTable
		/// <summary>
		/// 执行指定的SQL语句，返回DataSet
		/// </summary>
		/// <param name="sql">sql语句</param>
		/// <param name="dbParams">参数</param>
		/// <returns>DataSet</returns>
		public DataTable ExecuteDataTable(string sql, params DatabaseParameter[] dbParams)
		{
			DataTable dt = null;
			DataSet ds = ExecuteDataset(sql, dbParams);
			if (ds.Tables.Count > 0) {
				dt = ds.Tables[0];
				ds.Tables.RemoveAt(0);
				ds.Dispose();
			}
			return dt;
		}

		/// <summary>
		/// 对指定的SQL语句进行分页查询，返回DataTable
		/// </summary>
		/// <param name="sql">sql语句</param>
		/// <param name="dbParams">参数</param>
		/// <param name="pageSize">每页纪录数</param>
		/// <param name="pageIndex">第几页</param>
		/// <param name="pageCount">总页数</param>
		/// <param name="recordCount">总纪录数</param>
		/// <returns>返回DataTable</returns>
		public DataTable ExecuteDataTable(string sql, int pageSize, int pageIndex
			, out int pageCount, out int recordCount, params DatabaseParameter[] dbParams)
		{
			DataTable dt = null;
			pageCount = 0;
			recordCount = 0;
			if (string.IsNullOrEmpty(sql)) {
				return dt;
			}

			if (_dbType == DatabaseType.SQLServer
				&& Regex.IsMatch(sql, @"\border by\b", RegexOptions.IgnoreCase)
				&& !Regex.IsMatch(sql, @"\btop\b", RegexOptions.IgnoreCase)) {
				//处理SQLServer子查询语句中不能包含order by的问题
				sql = Regex.Replace(sql, @"^\s*select", "select top 100 percent", RegexOptions.IgnoreCase);
			}
			string sqlCount = string.Format("SELECT COUNT(1) FROM ({0}) rc", sql);

			recordCount = Convert.ToInt32(ExcuteScalar(sqlCount, dbParams));
			pageSize = pageSize > recordCount || pageSize <= 0 ? recordCount : pageSize;
			pageCount = pageSize != 0 ? Convert.ToInt32(Math.Ceiling(recordCount * 1.0 / pageSize)) : 0;
			pageIndex = pageIndex <= 0 ? 1 : pageIndex;
			pageIndex = pageIndex > pageCount ? pageCount : pageIndex;

			int startIndex = pageIndex > 0 ? (pageIndex - 1) * pageSize + 1 : 0;
			int endIndex = pageIndex * pageSize;
			switch (_dbType) {
				case DatabaseType.MySql:
					sql = string.Format("SELECT * FROM ({0}) tmp LIMIT {1}, {2}", sql, startIndex - 1, pageSize);
					dt = ExecuteDataTable(sql, dbParams);
					break;
				case DatabaseType.Oracle:
					sql = string.Format("SELECT * FROM (SELECT rownum rnum, tmp.* FROM ({0}) tmp) tmp WHERE rnum >= {1} AND rnum <= {2} ORDER BY rnum"
						, sql, startIndex, endIndex);
					dt = ExecuteDataTable(sql, dbParams);
					break;
				//case DatabaseType.SQLServer:	//SQLServer 排序存在问题
				//    sql = string.Format("SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY (SELECT 0)) AS rnum,* FROM ({0}) tmp) tmp WHERE rnum >={1} AND rnum <= {2} ORDER BY rnum"
				//        , sql, startIndex, endIndex);
				//    dt = ExecuteDataTable(sql, dbParams);
				//    break;
				default:
					IDataReader dr = ExecuteReader(sql, dbParams);
					dt = Paging(dr, pageSize, pageIndex);
					break;
			}
			if (dt != null && dt.Columns.Contains("RNUM")) {
				dt.Columns.Remove("RNUM");
			}
			return dt;
		}

		#endregion

		#region ExecuteReader
		/// <summary>
		/// 执行指定的SQL语句，返回IDataReader
		/// </summary>
		/// <param name="sql">sql语句</param>
		/// <param name="dbParams">参数</param>
		/// <returns>返回IDataReader</returns>
		public IDataReader ExecuteReader(string sql, params DatabaseParameter[] dbParams)
		{
			IDataReader odr;
			using (IDbCommand cmd = CreateCommand(sql, CommandType.Text, dbParams)) {
				try {
					if (_conn.State == ConnectionState.Closed) {
						_conn.Open();
					}
				}
				catch (Exception ex) {
					string error = string.Format("数据库连接失败：连接字符串为（{0}）,{1}"
						, _conn.ConnectionString, ex.Message);
					throw new Exception(error, ex);
				}

				try {
					odr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
				}
				catch (Exception ex) {
					string error = string.Format("SQL语句执行失败：（{0}）,{1}", cmd.CommandText, ex.Message);
					throw new Exception(error, ex);
				}
			}
			return odr;
		}

		#endregion

		#region ExecuteProcedure
		public void ExecuteProcedure(string procedureName, params DatabaseParameter[] dbParams)
		{
			using (IDbCommand cmd = CreateCommand(procedureName, CommandType.StoredProcedure, dbParams)) {
				try {
					if (_conn.State == ConnectionState.Closed) {
						_conn.Open();
					}
				}
				catch (Exception ex) {
					string error = string.Format("数据库连接失败：连接字符串为（{0}）,{1}"
						, _conn.ConnectionString, ex.Message);
					throw new Exception(error, ex);
				}

				try {
					cmd.ExecuteNonQuery();
					foreach (var outParam in dbParams.Where(item =>
						item.Direction == ParameterDirection.Output)) {
						outParam.Value = ((IDataParameter)cmd.Parameters[outParam.ParameterName]).Value;
					}
				}
				catch (Exception ex) {
					string error = string.Format("存储过程执行失败：（{0}）,{1}", cmd.CommandText, ex.Message);
					throw new Exception(error, ex);
				}
				finally {
					_conn.Close();
				}
			}
		}

		public T ExecuteProcedure<T>(string procedureName, params DatabaseParameter[] dbParams)
		{
			object returnValue = null;
			using (IDbCommand cmd = CreateCommand(procedureName, CommandType.StoredProcedure, dbParams)) {
				try {
					if (_conn.State == ConnectionState.Closed) {
						_conn.Open();
					}
				}
				catch (Exception ex) {
					string error = string.Format("数据库连接失败：连接字符串为（{0}）,{1}"
						, _conn.ConnectionString, ex.Message);
					throw new Exception(error, ex);
				}

				try {
					switch (_dbType) {
						case DatabaseType.SQLServer:
							if (typeof(T).IsAssignableFrom(typeof(DataSet))) {
								DataSet ds = new DataSet();
								IDbDataAdapter da = _provider.CreateDataAdapter();
								da.SelectCommand = cmd;
								da.Fill(ds);
								returnValue = ds;
							}
							else if (typeof(T).IsAssignableFrom(typeof(DataTable))) {
								DataSet ds = new DataSet();
								IDbDataAdapter da = _provider.CreateDataAdapter();
								da.SelectCommand = cmd;
								da.Fill(ds);
								returnValue = ds.Tables[0];
							}
							else if (typeof(T) == typeof(IDataReader)) {
								returnValue = cmd.ExecuteReader(CommandBehavior.CloseConnection);
							}
							else {
								returnValue = cmd.ExecuteScalar();
							}
							break;
						default:
							cmd.ExecuteNonQuery();
							break;
					}
					foreach (DatabaseParameter outParam in dbParams.Where(item =>
						item.Direction == ParameterDirection.Output)) {
						outParam.Value = ((IDataParameter)cmd.Parameters[outParam.ParameterName]).Value;
					}
				}
				catch (Exception ex) {
					string error = string.Format("存储过程执行失败：（{0}）,{1}", cmd.CommandText, ex.Message);
					throw new Exception(error, ex);
				}
				finally {
					if (typeof(T) != typeof(IDataReader)) {
						_conn.Close();
					}
				}

				try {
					return (T)returnValue;
				}
				catch (Exception ex) {
					string error = string.Format("存储过程（{0}）返回值的类型与指定的类型不一致", cmd.CommandText);
					throw new Exception(error, ex);
				}
			}
		}
		#endregion

		#region Paging
		/// <summary>
		/// 取某一页数据
		/// </summary>
		/// <param name="dataReader"></param>
		/// <param name="pageSize"></param>
		/// <param name="pageIndex"></param>
		/// <returns></returns>
		private static DataTable Paging(IDataReader dataReader, int pageSize, int pageIndex)
		{
			DataTable dt = new DataTable("Table");
			int colCount = dataReader.FieldCount;
			for (int i = 0; i < colCount; i++) {
				dt.Columns.Add(new DataColumn(dataReader.GetName(i), dataReader.GetFieldType(i)));
			}

			//将DataReader中的数据填充到DataTable中
			object[] values = new object[colCount];
			int iCount = 0;
			while (dataReader.Read()) {
				if (iCount >= pageSize * (pageIndex - 1) && iCount < pageSize * pageIndex) {
					dataReader.GetValues(values);
					dt.Rows.Add(values);
				}
				else if (iCount >= pageSize * pageIndex) {
					break;
				}
				iCount++;
			}

			if (!dataReader.IsClosed) {
				dataReader.Close();
				dataReader.Dispose();
			}
			return dt;
		}
		#endregion

		#region CreateCommand
		private IDbCommand CreateCommand(string sql, CommandType cmdType, params DatabaseParameter[] dbParams)
		{
			IDbCommand cmd = _provider.CreateCommand();
			cmd.Connection = _conn;
			cmd.CommandText = sql;
			cmd.CommandType = cmdType;

			if (dbParams != null) {
				foreach (var p in dbParams) {
					IDbDataParameter param = _provider.CreateDataParameter();
					param.ParameterName = p.ParameterName;
					param.DbType = p.DbType;
					param.Direction = p.Direction;
					if (param.Direction == ParameterDirection.Input)
						param.Value = p.Value;
					cmd.Parameters.Add(param);
				}
			}
			return cmd;
		}
		#endregion
	}
}
