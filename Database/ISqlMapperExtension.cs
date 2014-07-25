using System;
using System.Data;
using IBatisNet.DataMapper;
using IBatisNet.DataMapper.MappedStatements;
using IBatisNet.DataMapper.Scope;

namespace BigData.Server.DataAccessHelper.Database
{
	static class ISqlMapperExtension
	{
		/// <summary>
		/// 执行参数化的Sql语句，返回DataTable
		/// </summary>
		/// <param name="mapper"></param>
		/// <param name="statementName"></param>
		/// <param name="paramObject"></param>
		/// <returns></returns>
		public static DataTable QueryForDataTable(this ISqlMapper mapper, string statementName, object paramObject = null)
		{
			bool isSessionLocal = false;
			ISqlMapSession session = mapper.LocalSession;
			if (session == null) {
				session = new SqlMapSession(mapper);
				session.OpenConnection();
				isSessionLocal = true;
			}

			DataTable dataTable = null;
			try {
				dataTable = new DataTable(statementName);
				IMappedStatement mapStatement = mapper.GetMappedStatement(statementName);
				RequestScope request = mapStatement.Statement.Sql.GetRequestScope(mapStatement, paramObject, session);
				mapStatement.PreparedCommand.Create(request, session, mapStatement.Statement, paramObject);
				using (IDbCommand cmd = request.IDbCommand) {
					dataTable.Load(cmd.ExecuteReader());
				}
			}
			finally {
				if (isSessionLocal) {
					session.CloseConnection();
				}
			}
			return dataTable;
		}

		/// <summary>
		/// 返回分页的DataTable
		/// </summary>
		/// <param name="mapper"></param>
		/// <param name="statementName"></param>
		/// <param name="paramObject"></param>
		/// <param name="pageSize"></param>
		/// <param name="pageIndex"></param>
		/// <param name="pageCount"></param>
		/// <param name="recordCount"></param>
		/// <returns></returns>
		public static DataTable QueryForDataTable(this ISqlMapper mapper, string statementName, object paramObject
			, int pageSize, int pageIndex, out int pageCount, out int recordCount)
		{
			bool isSessionLocal = false;
			ISqlMapSession session = mapper.LocalSession;
			if (session == null) {
				session = new SqlMapSession(mapper);
				session.OpenConnection();
				isSessionLocal = true;
			}
			DataTable dt = null;
			pageCount = 0;
			recordCount = 0;

			try {
				IMappedStatement mapStatement = mapper.GetMappedStatement(statementName);
				RequestScope request = mapStatement.Statement.Sql.GetRequestScope(mapStatement, paramObject, session);
				mapStatement.PreparedCommand.Create(request, session, mapStatement.Statement, paramObject);
				using (IDbCommand cmd = request.IDbCommand) {
					string sql = cmd.CommandText;
					string sqlCount = string.Format("select count(1) from ({0})", sql);
					cmd.CommandText = sqlCount;
					recordCount = Convert.ToInt32(cmd.ExecuteScalar());
					pageSize = pageSize > recordCount || pageSize <= 0 ? recordCount : pageSize;
					pageCount = pageSize != 0 ? Convert.ToInt32(Math.Ceiling(recordCount * 1.0 / pageSize)) : 0;
					pageIndex = pageIndex <= 0 ? 1 : pageIndex;
					pageIndex = pageIndex > pageCount ? pageCount : pageIndex;

					cmd.CommandText = sql;
					IDataReader dr = cmd.ExecuteReader();
					dt = Paging(dr, pageSize, pageIndex);
					dt.TableName = statementName;
				}
			}
			finally {
				if (isSessionLocal) {
					session.CloseConnection();
				}
			}
			return dt;
		}

		/// <summary>
		/// 执行参数化的Sql语句，返回DataSet
		/// </summary>
		/// <param name="mapper"></param>
		/// <param name="statementName"></param>
		/// <param name="paramObject"></param>
		/// <returns></returns>
		public static DataSet QueryForDataSet(this ISqlMapper mapper, string statementName, object paramObject = null)
		{
			bool isSessionLocal = false;
			ISqlMapSession session = mapper.LocalSession;
			if (session == null) {
				session = new SqlMapSession(mapper);
				session.OpenConnection();
				isSessionLocal = true;
			}

			DataSet ds = new DataSet(statementName);
			try {
				IMappedStatement mapStatement = mapper.GetMappedStatement(statementName);
				RequestScope request = mapStatement.Statement.Sql.GetRequestScope(mapStatement, paramObject, session);
				mapStatement.PreparedCommand.Create(request, session, mapStatement.Statement, paramObject);
				using (IDbCommand cmd = request.IDbCommand) {
					IDataReader reader = cmd.ExecuteReader();
					DataTable dt = new DataTable();
					while (!reader.IsClosed) {
						dt.Load(reader);
					}
					ds.Tables.Add(dt);
				}

				//以下是利用反射实现
				//System.Reflection.FieldInfo info = request.IDbCommand.GetType().GetField("_innerDbCommand", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
				//using (IDbCommand cmd = (IDbCommand)info.GetValue(request.IDbCommand))
				//{
				//    session.CreateDataAdapter(cmd).Fill(ds);
				//}
			}
			finally {
				if (isSessionLocal) {
					session.CloseConnection();
				}
			}
			return ds;
		}

		/// <summary>
		/// 取某一页数据
		/// </summary>
		/// <param name="dataReader"></param>
		/// <param name="pageSize"></param>
		/// <param name="pageIndex"></param>
		/// <returns></returns>
		private static DataTable Paging(IDataReader dataReader, int pageSize, int pageIndex)
		{
			DataTable dt = new DataTable();
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
	}
}
