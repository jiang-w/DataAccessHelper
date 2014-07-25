using System;

namespace BigData.Server.DataAccessHelper.DataEntity
{
	/// <summary>
	/// 字段值特性
	/// </summary>
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
	public class FieldValueAttribute : Attribute
	{
		private readonly FieldValueType _type = FieldValueType.String;
		/// <summary>
		/// 字段值类型
		/// </summary>
		public FieldValueType ValueType
		{
			get { return _type; }
		}

		private readonly object _defaultVal = null;
		/// <summary>
		/// 字段默认值
		/// </summary>
		public object DefaultValue
		{
			get { return _defaultVal; }
		}

		public FieldValueAttribute(FieldValueType type)
		{
			_type = type;
		}

		public FieldValueAttribute(FieldValueType type, object defaultVal)
			: this(type)
		{
			_defaultVal = defaultVal;
		}
	}
}
