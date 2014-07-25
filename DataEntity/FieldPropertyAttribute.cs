using System;

namespace BigData.Server.DataAccessHelper.DataEntity
{
	/// <summary>
	/// 字段特性
	/// </summary>
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
	public class FieldPropertyAttribute : Attribute
	{
		private bool _isKey = false;
		/// <summary>
		/// 是否为主键，默认值false
		/// </summary>
		public bool IsKey
		{
			get { return _isKey; }
			set { _isKey = value; }
		}

		private bool _isNullable = true;
		/// <summary>
		/// 是否允许为空，默认值true
		/// </summary>
		public bool IsNullable
		{
			get
			{
				return !IsKey && _isNullable;
			}
			set { _isNullable = value; }
		}

		private string _fieldName;
		/// <summary>
		/// 实体类属性对应数据库表字段名
		/// </summary>
		public string FieldName
		{
			get { return _fieldName; }
			private set
			{
				if (string.IsNullOrWhiteSpace(value))
					throw new ArgumentException("Value cannot be null or empty.", "name");
				_fieldName = value;
			}
		}

		public FieldPropertyAttribute(string name)
		{
			this.FieldName = name;
		}
	}
}
