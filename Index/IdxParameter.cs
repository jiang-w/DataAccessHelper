using System;

namespace BigData.Server.DataAccessHelper.Index
{
	/// <summary>
	/// 指标参数结构定义
	/// </summary>
	public struct IdxParameter : IComparable<IdxParameter>
	{
		public string Name { get; set; }
		public string Type { get; set; }
		public object Value { get; set; }
		public string DefaultValue { get; set; }
		public string Description { get; set; }
		public string DefaultValueName { get; set; }

		public int CompareTo(IdxParameter other)
		{
			return Name.CompareTo(other.Name);
		}
	}
}
