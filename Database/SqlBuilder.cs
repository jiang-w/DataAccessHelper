using System.Linq;
using System.Text.RegularExpressions;
using BigData.Server.DataAccessHelper.Utility;

namespace BigData.Server.DataAccessHelper.Database
{
	public class SqlBuilder
	{
		#region 字段、属性
		/// <summary>
		/// 数据库类型
		/// </summary>
		public DatabaseType DbType { get; set; }

		/// <summary>
		/// 表或查询语句
		/// </summary>
		public string TableOrQuery { get; set; }

		/// <summary>
		/// 获取、设置返回的纪录数
		/// </summary>
		public uint RecordCount { get; set; }

		/// <summary>
		/// 查找的字段
		/// </summary>
		public SqlFields Fields { get; set; }

		/// <summary>
		/// 过滤条件
		/// </summary>
		public UQuery Filter { get; set; }

		/// <summary>
		/// 排序
		/// </summary>
		public SortField[] Sort { get; set; }
		#endregion

		#region 构造函数
		public SqlBuilder(string tableOrQuery, DatabaseType dbType)
		{
			this.DbType = dbType;
			this.TableOrQuery = tableOrQuery.Trim();
		}
		#endregion

		/// <summary>
		/// 构造SQL语句
		/// </summary>
		/// <returns></returns>
		public string Build()
		{
			#region 构造Select语句块
			string selectSql = "SELECT " + (this.Fields != null ? this.Fields.GenerateSql(this.DbType) : "*");
			#endregion

			#region 构造From语句块
			Regex regex = new Regex(@"\bselect", RegexOptions.IgnoreCase);
			string fromSql = regex.IsMatch(this.TableOrQuery) ? string.Format(" FROM ({0}) BUILD", this.TableOrQuery) : string.Format(" FROM {0}", this.TableOrQuery);
			#endregion

			#region 构造Where语句块
			string whereSql = this.Filter != null ? this.Filter.GenerateSql(this.DbType) : string.Empty;
			if (!string.IsNullOrEmpty(whereSql))
				whereSql = " WHERE " + whereSql;
			#endregion

			#region 构造Order By语句块
			string orderBySql;
			if (this.Sort != null)
				orderBySql = string.Join(", ", this.Sort.Select(sort => string.Format("{0} {1}", sort.FieldName, sort.SortMode)));
			else
				orderBySql = string.Empty;
			if (!string.IsNullOrEmpty(orderBySql))
				orderBySql = " ORDER BY " + orderBySql;
			#endregion

			string sql = selectSql + fromSql + whereSql + orderBySql;

			#region 返回的记录数
			if (this.RecordCount > 0) {
				switch (this.DbType) {
					case DatabaseType.Oracle: {
							sql = string.Format("SELECT * FROM ({0}) WHERE ROWNUM <= {1} ORDER BY ROWNUM", sql, this.RecordCount);
							break;
						}
					case DatabaseType.SQLServer: {
							sql = string.Format("SELECT TOP {0} * FROM ({1})", this.RecordCount, sql);
							break;
						}
					case DatabaseType.MySql: {
							sql = string.Format("SELECT * FROM ({1}) LIMIT {0}", this.RecordCount, sql);
							break;
						}
				}
			}
			#endregion

			return sql;
		}
	}
}
