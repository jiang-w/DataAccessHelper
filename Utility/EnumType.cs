using System;

namespace BigData.Server.DataAccessHelper.Utility
{
	/// <summary>
	/// UtilityQuery的查询类型
	/// </summary>
	enum QueryType
	{
		Greater,
		GreaterOrEqual,
		Less,
		LessOrEqual,
		Equal,
		NotEqual,
		In,
		NotIn,
		Like
	}

	/// <summary>
	/// UtilityQuery间的关系类型
	/// </summary>
	enum RelationType
	{
		And,
		Or
	}

	/// <summary>
	/// 排序方式（ASC 升序，DESC 降序）
	/// </summary>
	public enum SortMode
	{
		ASC,
		DESC
	}

	#region Enum Class Extensions
	static class QueryTypeExtensions
	{
		public static string GetJsonPropertyName(this QueryType type)
		{
			switch (type) {
				case QueryType.Equal:
					return string.Empty;
				case QueryType.NotEqual:
					return "$ne";
				case QueryType.Greater:
					return "$gt";
				case QueryType.GreaterOrEqual:
					return "$gte";
				case QueryType.In:
					return "$in";
				case QueryType.NotIn:
					return "$nin";
				case QueryType.Less:
					return "$lt";
				case QueryType.LessOrEqual:
					return "$lte";
				case QueryType.Like:
					return "$lk";
				default:
					return type.ToString();
			}
		}

		public static QueryType GetTypeByPropertyName(string name)
		{
			switch (name) {
				case "$ne": return QueryType.NotEqual;
				case "$gt": return QueryType.Greater;
				case "$gte": return QueryType.GreaterOrEqual;
				case "$lt": return QueryType.Less;
				case "$lte": return QueryType.LessOrEqual;
				case "$in": return QueryType.In;
				case "$nin": return QueryType.NotIn;
				case "$lk": return QueryType.Like;
				default: throw new Exception(string.Format("字符串{0}无法转换为QueryType类型枚举值", name));
			}
		}
	}

	static class RelationTypeExtensions
	{
		public static string GetJsonPropertyName(this RelationType type)
		{
			switch (type) {
				case RelationType.Or:
					return "$or";
				case RelationType.And:
					return "$and";
				default:
					return "$and";
			}
		}

		public static RelationType GetTypeByPropertyName(string name)
		{
			switch (name) {
				case "$or": return RelationType.Or;
				case "$and": return RelationType.And;
				default: throw new Exception(string.Format("字符串{0}无法转换为RelationType类型枚举值", name));
			}
		}
	}
	#endregion
}