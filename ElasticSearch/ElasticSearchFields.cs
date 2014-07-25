using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BigData.Server.DataAccessHelper.ElasticSearch
{
	public class ElasticSearchFields
	{
		private List<string> _fields = new List<string>();
		public IEnumerable<string> Fields { get { return _fields; } }

		public ElasticSearchFields Add(string fieldName)
		{
			_fields.Add(fieldName);
			return this;
		}

		public ElasticSearchFields AddRange(params string[] fieldNames)
		{
			var list = fieldNames.Where(s => !string.IsNullOrWhiteSpace(s));
			_fields.AddRange(list);
			return this;
		}

		public ElasticSearchFields AddRange(IEnumerable<string> fieldNames)
		{
			var list = fieldNames.Where(s => !string.IsNullOrWhiteSpace(s));
			_fields.AddRange(list);
			return this;
		}

		public ElasticSearchFields Remove(string fieldName)
		{
			_fields.Remove(fieldName);
			return this;
		}
	}
}
