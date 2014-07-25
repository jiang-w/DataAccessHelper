using System;
using System.Linq;
using BigData.Server.DataAccessHelper.Utility;

namespace BigData.Server.DataAccessHelper.ElasticSearch
{
	public static class IQueryExtensions
	{
		public static ElasticSearchFilter ConvertToElasticSearchFilter(this UQuery query)
		{
			ElasticSearchFilter esFilter = null;
			if (query is FieldQuery) {
				FieldQuery fieldQuery = query as FieldQuery;
				switch (fieldQuery.Type) {
					case QueryType.Equal:
						esFilter = ElasticSearchFilter.Eq(fieldQuery.Key, fieldQuery.Value);
						break;
					case QueryType.NotEqual:
						esFilter = ElasticSearchFilter.NotEq(fieldQuery.Key, fieldQuery.Value);
						break;
					case QueryType.Like:
						esFilter = ElasticSearchFilter.Lk(fieldQuery.Key, fieldQuery.Value.ToString());
						break;
					case QueryType.In:
						esFilter = ElasticSearchFilter.In(fieldQuery.Key, fieldQuery.Value as Array);
						break;
					case QueryType.Greater:
					case QueryType.GreaterOrEqual:
					case QueryType.Less:
					case QueryType.LessOrEqual:
					case QueryType.NotIn:
						throw new NotSupportedException(string.Format("ElasticSearchFilter不支持{0}操作符"
							, fieldQuery.Type.ToString()));
				}
			}
			else if (query is RelationQuery) {
				RelationQuery rq = query as RelationQuery;
				switch (rq.Relation) {
					case RelationType.And:
						esFilter = ElasticSearchFilter.And(rq.Queries.Select(q => q.ConvertToElasticSearchFilter()).ToArray());
						break;
					case RelationType.Or:
						esFilter = ElasticSearchFilter.Or(rq.Queries.Select(q => q.ConvertToElasticSearchFilter()).ToArray());
						break;
				}
			}
			return esFilter;
		}
	}
}
