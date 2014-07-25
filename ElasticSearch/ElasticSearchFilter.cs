using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ElasticSearch.Client.QueryDSL;

namespace BigData.Server.DataAccessHelper.ElasticSearch
{
	public class ElasticSearchFilter
	{
		internal IQuery Query { get; private set; }

		private ElasticSearchFilter(IQuery query)
		{
			Query = query;
		}

		public static ElasticSearchFilter Eq(string fieldName, object value)
		{
			TermQuery query = new TermQuery(fieldName, value);
			return new ElasticSearchFilter(query);
		}

		public static ElasticSearchFilter NotEq(string fieldName, object value)
		{
			BoolQuery query = new BoolQuery().MustNot(new TermQuery(fieldName, value));
			return new ElasticSearchFilter(query);
		}

		public static ElasticSearchFilter Lk(string fieldName, string value)
		{
			TextQuery query = new TextQuery(fieldName, value, Operator.AND, "ik");
			return new ElasticSearchFilter(query);
		}

		public static ElasticSearchFilter And(params ElasticSearchFilter[] filters)
		{
			IEnumerable<ElasticSearchFilter> queryList = filters.Where(item => item != null);
			if (queryList.Count() == 0)
				return null;

			BoolQuery query = new BoolQuery();

			List<IQuery> iQueryList = new List<IQuery>();

			foreach (ElasticSearchFilter q in queryList) {
				iQueryList.Add(q.Query);
			}

			query.Must(iQueryList.ToArray());
			return new ElasticSearchFilter(query);
		}

		public static ElasticSearchFilter In(string fieldName, Array values)
		{
			StringBuilder sb = new StringBuilder();
			foreach (var item in values) {
				if (sb.Length == 0) {
					sb.Append(item);
				}
				else {
					sb.AppendFormat(" OR {0} ", item);
				}
			}
			FieldQuery query = new FieldQuery(fieldName, sb.ToString());
			return new ElasticSearchFilter(query);
		}

		public static ElasticSearchFilter Between(string fieldName, int from, int to)
		{
			if (from >= to) {
				throw new ArgumentException("the value of argument from must less than argument to");
			}
			RangeQuery range = new RangeQuery(fieldName, from.ToString(), to.ToString(), true, true);
			return new ElasticSearchFilter(range);
		}

		public static ElasticSearchFilter Or(params ElasticSearchFilter[] filters)
		{
			IEnumerable<ElasticSearchFilter> queryList = filters.Where(item => item != null);
			if (queryList.Count() == 0)
				return null;

			BoolQuery query = new BoolQuery();

			List<IQuery> iQueryList = new List<IQuery>();

			foreach (ElasticSearchFilter q in queryList) {
				iQueryList.Add(q.Query);
			}

			query.Should(iQueryList.ToArray());
			return new ElasticSearchFilter(query);
		}
	}
}
