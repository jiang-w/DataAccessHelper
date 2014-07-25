using System.Collections.Generic;
using System.Linq;

namespace BigData.Server.DataAccessHelper.Database
{
	public class SqlFields
	{
		struct Field
		{
			public string Name { get; set; }
			public string Alias { get; set; }
		}

		readonly List<Field> _fieldList = new List<Field>();

		public int Count
		{
			get { return _fieldList.Count; }
		}

		public SqlFields Add(params string[] names)
		{
			if (names != null) {
				names.Where(item => !string.IsNullOrWhiteSpace(item)).Select(item => item.Trim()).Distinct()
					.ToList().ForEach(name => {
						_fieldList.Add(new Field { Name = name, Alias = string.Empty });
					});
			}
			return this;
		}

		public SqlFields Add(params KeyValuePair<string, string>[] fields)
		{
			if (fields != null) {
				_fieldList.AddRange(fields.Select(field => new Field { Name = field.Key, Alias = field.Value }));
			}
			return this;
		}

		public SqlFields Add(IEnumerable<KeyValuePair<string, string>> fields)
		{
			if (fields != null) {
				_fieldList.AddRange(fields.Select(field => new Field { Name = field.Key, Alias = field.Value }));
			}
			return this;
		}

		public void Clear()
		{
			_fieldList.Clear();
		}

		public string GenerateSql(DatabaseType dbType)
		{
			string[] fieldsSql = _fieldList.Select(field => string.Format("{0} {1}"
				, field.Name, field.Alias).Trim()).ToArray();
			if (fieldsSql.Length > 0) {
				return string.Join(", ", fieldsSql);
			}
			else {
				return "*";
			}
		}
	}
}
