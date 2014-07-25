using System;
using System.Linq;
using System.Text.RegularExpressions;
using BigData.Server.DataAccessHelper.Utility;

namespace BigData.Server.DataAccessHelper.MongoDb
{
	public static class IQueryExtensions
	{
		public static MongoDBFilter ConvertToMongoDBFilter(this UQuery query)
		{
			MongoDBFilter mongoFilter = null;
			if (query is FieldQuery) {
				FieldQuery fieldQuery = query as FieldQuery;
				switch (fieldQuery.Type) {
					case QueryType.Equal:
						mongoFilter = MongoDBFilter.Eq(fieldQuery.Key, fieldQuery.Value);
						break;
					case QueryType.NotEqual:
						mongoFilter = MongoDBFilter.NotEq(fieldQuery.Key, fieldQuery.Value);
						break;
					case QueryType.Greater:
						mongoFilter = MongoDBFilter.Gt(fieldQuery.Key, fieldQuery.Value);
						break;
					case QueryType.GreaterOrEqual:
						mongoFilter = MongoDBFilter.Ge(fieldQuery.Key, fieldQuery.Value);
						break;
					case QueryType.Less:
						mongoFilter = MongoDBFilter.Lt(fieldQuery.Key, fieldQuery.Value);
						break;
					case QueryType.LessOrEqual:
						mongoFilter = MongoDBFilter.Le(fieldQuery.Key, fieldQuery.Value);
						break;
					case QueryType.Like:
						mongoFilter = MongoDBFilter.Lk(fieldQuery.Key, new Regex(fieldQuery.Value.ToString()));
						break;
					case QueryType.In:
						mongoFilter = MongoDBFilter.In(fieldQuery.Key, fieldQuery.Value as Array);
						break;
					case QueryType.NotIn:
						mongoFilter = MongoDBFilter.NotIn(fieldQuery.Key, fieldQuery.Value as Array);
						break;
				}
			}
			else if (query is RelationQuery) {
				RelationQuery rq = query as RelationQuery;
				switch (rq.Relation) {
					case RelationType.And:
						mongoFilter = MongoDBFilter.And(rq.Queries.Select(q => q.ConvertToMongoDBFilter()).ToArray());
						break;
					case RelationType.Or:
						mongoFilter = MongoDBFilter.Or(rq.Queries.Select(q => q.ConvertToMongoDBFilter()).ToArray());
						break;
				}
			}
			return mongoFilter;
		}
	}
}
