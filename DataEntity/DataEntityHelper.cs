using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using BigData.Server.DataAccessHelper.Database;
using BigData.Server.DataAccessHelper.Utility;

namespace BigData.Server.DataAccessHelper.DataEntity
{
	public class DataEntityHelper
	{
		#region Fields
		private readonly DatabaseConnection _conn;
		#endregion

		#region Constructor
		public DataEntityHelper(DatabaseConnection conn)
		{
			_conn = conn;
		}
		#endregion

		#region InsertOrUpdate

		/// <summary>
		/// 通过数据实体向数据库更新数据
		/// </summary>
		/// <param name="entity"></param>
		/// <returns></returns>
		public int Update(EntityBase entity)
		{
			DbTableInfo tableInfo = entity.AnalyzeTableInfo();
			var updateFields = tableInfo.Fields.Where(item => !item.IsKey && !item.IsIgnoreUpdate);
			var filterFields = tableInfo.Fields.Where(item => item.IsKey);
			if (filterFields.Count() == 0)
				throw new Exception(string.Format("{0} class error:Primary key attribute is missing", entity.GetType().Name));
			string sql = string.Format("UPDATE {0} SET {1} WHERE {2}"
				, tableInfo.Name
				, string.Join(", ", updateFields.Select(item => {
					if (item.ValueType == FieldValueType.Function) {
						return string.Format("{0} = {1}", item.Name, item.DefaultValue);
					}
					else {
						return string.Format("{0} = {1}{0}", item.Name, _conn.Provider.ParameterPrefix);
					}
				}))
				, string.Join(" AND ", filterFields.Select(item => string.Format("{0} = {1}{0}"
					, item.Name, _conn.Provider.ParameterPrefix))));
			DatabaseParameter[] dbParams = updateFields.Where(item => item.ValueType != FieldValueType.Function)
				.Select(item => {
					string paramName = item.Name;
					object paramValue = (item.Value ?? item.DefaultValue) ?? DBNull.Value;
					return new DatabaseParameter(paramName, paramValue);
				})
				.Concat(filterFields.Select(item => new DatabaseParameter(item.Name, item.Value))).ToArray();
			DatabaseAccess dba = new DatabaseAccess(_conn);
			return dba.ExecuteNonQuery(sql, dbParams);
		}

		/// <summary>
		/// 通过数据实体向数据库插入数据
		/// </summary>
		/// <param name="entity"></param>
		/// <returns></returns>
		public int Insert(EntityBase entity)
		{
			DbTableInfo tableInfo = entity.AnalyzeTableInfo();
			string sql = string.Format("INSERT INTO {0} ({1}) VALUES ({2})"
				, tableInfo.Name
				, string.Join(", ", tableInfo.Fields.Select(item => string.Format("{0}", item.Name)))
				, string.Join(", ", tableInfo.Fields.Select(item => {
					if (item.ValueType == FieldValueType.Function) {
						return item.DefaultValue.ToString();
					}
					else {
						return string.Format("{1}{0}", item.Name, _conn.Provider.ParameterPrefix);
					}
				})));
			DatabaseParameter[] dbParams = tableInfo.Fields.Where(item => item.ValueType != FieldValueType.Function)
				.Select(item => {
					string paramName = item.Name;
					object paramValue = (item.Value ?? item.DefaultValue) ?? DBNull.Value;
					return new DatabaseParameter(paramName, paramValue);
				}).ToArray();
			DatabaseAccess dba = new DatabaseAccess(_conn);
			return dba.ExecuteNonQuery(sql, dbParams);
		}

		/// <summary>
		/// 通过数据实体向数据库插入或更新数据
		/// </summary>
		/// <param name="entity"></param>
		/// <returns></returns>
		public int InsertOrUpdate(EntityBase entity)
		{
			DbTableInfo tableInfo = entity.AnalyzeTableInfo();
			var filterFields = tableInfo.Fields.Where(item => item.IsKey);
			string sql = string.Format("SELECT COUNT(1) FROM {0} WHERE {1}"
				, tableInfo.Name
				, string.Join(" AND ", filterFields.Select(itme => string.Format("{0} = {1}{0}"
								, itme.Name, _conn.Provider.ParameterPrefix))));
			DatabaseParameter[] dbParams = filterFields.Select(item =>
				new DatabaseParameter(item.Name, item.Value)).ToArray();
			DatabaseAccess dba = new DatabaseAccess(_conn);
			int count = Convert.ToInt32(dba.ExcuteScalar(sql, dbParams));
			if (count > 0) {
				return Update(entity);
			}
			else {
				return Insert(entity);
			}
		}

		#endregion

		#region Delete

		/// <summary>
		/// 从数据库中删除实体对应的数据
		/// </summary>
		/// <param name="entity"></param>
		/// <returns></returns>
		public int Delete(EntityBase entity)
		{
			if (entity == null)
				throw new ArgumentNullException("entity", "The data entity cannot be null");
			DbTableInfo tableInfo = entity.AnalyzeTableInfo();
			var filterFields = tableInfo.Fields.Where(item => item.IsKey);
			if (filterFields.Count() == 0)
				throw new Exception(string.Format("{0} class error:Primary key attribute is missing", entity.GetType().Name));
			string sql = string.Format("DELETE FROM {0} WHERE {1}"
				, tableInfo.Name
				, string.Join(" AND ", filterFields.Select(itme => string.Format("{0} = {1}{0}"
								, itme.Name, _conn.Provider.ParameterPrefix))));
			DatabaseParameter[] dbParams = filterFields.Select(item =>
				new DatabaseParameter(item.Name, item.Value)).ToArray();
			DatabaseAccess dba = new DatabaseAccess(_conn);
			return dba.ExecuteNonQuery(sql, dbParams);
		}

		#endregion

		#region Query

		/// <summary>
		/// 根据指定的查询对象，返回第一个数据实体对象
		/// </summary>
		/// <typeparam name="T">数据实体类型，继承自EntityBase</typeparam>
		/// <param name="query">指定查询</param>
		/// <returns>数据实体对象</returns>
		public T FindOne<T>(UQuery query) where T : EntityBase
		{
			return FindAll<T>(query).FirstOrDefault();
		}

		/// <summary>
		/// 根据指定的查询对象，返回数据实体对象列表
		/// </summary>
		/// <typeparam name="T">数据实体类型，继承自EntityBase</typeparam>
		/// <param name="query">指定查询</param>
		/// <returns>数据实体对象列表</returns>
		public List<T> FindAll<T>(UQuery query) where T : EntityBase
		{
			Type entityType = typeof(T);
			TablePropertyAttribute tableProperty = Attribute.GetCustomAttribute(entityType, typeof(TablePropertyAttribute)) as TablePropertyAttribute;
			string tableName = tableProperty.TableName;
			SqlBuilder builder = new SqlBuilder(tableName, _conn.DatabaseType) { Filter = query };
			string sql = builder.Build();
			DatabaseAccess dba = new DatabaseAccess(_conn);
			return FillEntitiesByDataReader<T>(dba.ExecuteReader(sql));
		}

		/// <summary>
		/// 根据指定的sql语句，返回一个数据实体对象
		/// </summary>
		/// <typeparam name="T">数据实体类型，继承自EntityBase</typeparam>
		/// <param name="sql">指定的sql语句</param>
		/// <returns>数据实体对象</returns>
		public T FindOne<T>(string sql) where T : EntityBase
		{
			return FindAll<T>(sql).FirstOrDefault();
		}

		/// <summary>
		/// 根据指定的sql语句，返回第一个数据实体对象列表
		/// </summary>
		/// <typeparam name="T">数据实体类型，继承自EntityBase</typeparam>
		/// <param name="sql">指定的sql语句</param>
		/// <returns>数据实体对象列表</returns>
		public List<T> FindAll<T>(string sql) where T : EntityBase
		{
			DatabaseAccess dba = new DatabaseAccess(_conn);
			var reader = dba.ExecuteReader(sql);
			List<T> entities = FillEntitiesByDataReader<T>(reader);
			return entities;
		}

		/// <summary>
		/// 将数据填充到数据实体
		/// </summary>
		/// <typeparam name="T">数据实体类型，继承自EntityBase</typeparam>
		/// <param name="reader"></param>
		/// <returns></returns>
		private static List<T> FillEntitiesByDataReader<T>(IDataReader reader) where T : EntityBase
		{
			List<T> entities = new List<T>();
			while (reader.Read()) {
				T instance = Activator.CreateInstance<T>();
				for (int i = 0; i < reader.FieldCount; i++) {
					string fieldName = reader.GetName(i);
					string propertyName = instance.FindPropertyNameByFieldName(fieldName);
					if (!string.IsNullOrEmpty(propertyName)) {
						object value = reader[i] == DBNull.Value ? null : reader[i];
						try {
							typeof(T).GetProperty(propertyName).SetValue(instance, value, null);
						}
						catch (Exception ex) {
							reader.Close();
							throw new Exception(string.Format("{0} 对象属性“{1}”赋值：{2}"
								, typeof(T).Name, propertyName, ex.Message));
						}
					}
				}
				entities.Add(instance);
			}
			reader.Close();
			return entities;
		}

		#endregion
	}
}
