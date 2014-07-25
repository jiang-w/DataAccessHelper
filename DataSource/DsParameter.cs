using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Collections.Generic;

namespace BigData.Server.DataAccessHelper.DataSource
{
	/// <summary>
	/// 数据源内部参数结构定义
	/// </summary>
	public struct DsParameter : IComparable<DsParameter>
	{
		public string Name { get; set; }
		public string Type { get; set; }
		public object Value { get; set; }
		public string DefaultValue { get; set; }
		public string Description { get; set; }
		public string DefaultValueName { get; set; }

		public int CompareTo(DsParameter other)
		{
			return Name.CompareTo(other.Name);
		}

		public static object ParseValue(string s)
		{
			if (string.IsNullOrWhiteSpace(s)) {
				return null;
			}

			var vals = s.Split(',').Where(item => !string.IsNullOrWhiteSpace(item))
				.Select(item => item.Trim()).Distinct();
			if (vals.Count() > 1) {
				return vals.Select(item => ParseValue(item));
			}

			if (Regex.IsMatch(s, @"^date'\d{4}-\d{1,2}-\d{1,2}'$"))//日期型字符串date'2011-01-30'
            {
				return Convert.ToDateTime(Regex.Match(s, @"\d{4}-\d{1,2}-\d{1,2}").Value);
			}
			else if (Regex.IsMatch(s, @"^'.*'$"))//字符串类型'abc'
            {
				return s.Substring(1, s.Length - 2);
			}
			else if (Regex.IsMatch(s, @"^-?([1-9]\d*|0)\.\d*$"))//小数 1.034
            {
				return Convert.ToDouble(s);
			}
			else if (Regex.IsMatch(s, @"^(-?[1-9]\d*|0)$"))//整数 123
            {
				int intValue;
				if (int.TryParse(s, out intValue))
					return intValue;
				else
					return Convert.ToInt64(s);
			}
			else {
				DataSourceFunction dsFunc = new DataSourceFunction();
				if (dsFunc.IsFunction(s)) {
					return ParseValue(dsFunc.ScalarFunction(s));
				}
				return s;
			}
		}
	}
}
