using System;
using System.Linq;
using System.Text;
using BigData.Server.DataAccessHelper.Utility;

namespace BigData.Server.DataAccessHelper.Database
{
	static class IQueryExtensions
	{
		#region 根据数据库类型构造SQL查询条件（where之后的部分）
		/// <summary>
		/// 根据数据库类型生成相应的查询条件
		/// </summary>
		/// <param name="query"></param>
		/// <param name="dbType"></param>
		/// <returns></returns>
		public static string GenerateSql(this UQuery query, DatabaseType dbType)
		{
			string sql = string.Empty;
			if (query is RelationQuery) {
				RelationQuery rq = query as RelationQuery;
				if (rq.Relation == RelationType.Or) {
					sql = String.Format("({0})", string.Join(" OR ", rq.Queries.Select(q => q.GenerateSql(dbType))));
				}
				else if (rq.Relation == RelationType.And) {
					sql = string.Join(" AND ", rq.Queries.Select(q => q.GenerateSql(dbType)));
				}
			}
			else if (query is FieldQuery) {
				FieldQuery fieldQuery = query as FieldQuery;
				if (fieldQuery.Type == QueryType.Like) {
					string regexString = fieldQuery.Value.ToString().Replace("'", "''");
					switch (dbType) {
						case DatabaseType.Oracle:
							sql = string.Format("REGEXP_LIKE({0},'{1}')", fieldQuery.Key, regexString);
							break;
						case DatabaseType.SQLServer:
							sql = string.Format("dbo.RegexMatch({0},'{1}','true') = 1", fieldQuery.Key, regexString);
							break;
						case DatabaseType.MySql:
						default:
							sql = string.Format("{0} LIKE '%{1}%'", fieldQuery.Key, regexString);
							break;
					}
				}
				else {
					string leftOperValue = FormatLeftOperValue(fieldQuery, dbType);
					string operString = FormatOperString(fieldQuery);
					string rightOperValue = FormatRightOperValue(fieldQuery.Value, dbType);
					sql = string.Format("{0} {1} {2}", leftOperValue, operString, rightOperValue);
				}
			}
			return sql;
		}

		private static string FormatOperString(FieldQuery query)
		{
			string operStr = string.Empty;
			switch (query.Type) {
				case QueryType.Equal:
					operStr = query.Value == null ? "IS" : "=";
					break;
				case QueryType.NotEqual:
					operStr = query.Value == null ? "NOT IS" : "<>";
					break;
				case QueryType.Greater: operStr = ">";
					break;
				case QueryType.GreaterOrEqual: operStr = ">=";
					break;
				case QueryType.Less: operStr = "<";
					break;
				case QueryType.LessOrEqual: operStr = "<=";
					break;
				case QueryType.In: operStr = "IN";
					break;
				case QueryType.NotIn: operStr = "NOT IN";
					break;
				case QueryType.Like: operStr = "LIKE";
					break;
			}
			return operStr;
		}

		private static string FormatRightOperValue(object value, DatabaseType dbType)
		{
			if (value is Array) {
				StringBuilder sqlBuilder = new StringBuilder();
				var values = value as Array;
				sqlBuilder.Append("(");
				bool first = true;
				foreach (var v in values) {
					if (first)
						first = false;
					else
						sqlBuilder.Append(",");
					sqlBuilder.Append(FormatRightOperValue(v, dbType));
				}
				sqlBuilder.Append(")");
				return sqlBuilder.ToString();
			}
			else {
				string rightOperValue;
				if (value == null) {
					rightOperValue = "NULL";
				}
				else if (value is DateTime) {
					switch (dbType) {
						case DatabaseType.Oracle:
							rightOperValue = string.Format("TO_DATE('{0}','YYYY/MM/DD HH24:MI:SS')"
									, Convert.ToDateTime(value).ToString("yyyy/MM/dd hh:mm:ss"));
							break;
						case DatabaseType.MySql:
							rightOperValue = string.Format("STR_TO_DATE('{0}','%Y-%m-%d %k:%i:%s')"
								, Convert.ToDateTime(value));
							break;
						case DatabaseType.SQLServer:
							rightOperValue = string.Format("'{0}'", Convert.ToDateTime(value).ToString("yyyyMMdd"));
							break;
						default:
							rightOperValue = string.Format("'{0}'", value);
							break;
					}
				}
				else if (value is string) {
					rightOperValue = string.Format("'{0}'", value.ToString().Replace("'", "''"));
				}
				else {
					rightOperValue = value.ToString();
				}
				return rightOperValue;
			}
		}

		private static string FormatLeftOperValue(FieldQuery query, DatabaseType dbType)
		{
			string leftOperValue = query.Key;
			if (dbType == DatabaseType.SQLServer) {
				if (query.Value is DateTime) {
					leftOperValue = string.Format("CONVERT(VARCHAR(8),[{0}],12)", query.Key);
				}
				else if (query.Value is Array) {
					foreach (var v in query.Value as Array) {
						if (!(v is DateTime))
							return leftOperValue;
					}
					leftOperValue = string.Format("CONVERT(VARCHAR(8),[{0}],12)", query.Key);
				}
			}
			return leftOperValue;
		}

		#endregion
	}
}