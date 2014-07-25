using System;

namespace BigData.Server.DataAccessHelper.DataEntity
{
	/// <summary>
	/// 表特性
	/// </summary>
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
	public class TablePropertyAttribute : Attribute
	{
		private string _tableName;
		/// <summary>
		/// 实体类对应数据库表名
		/// </summary>
		public string TableName
		{
			get
			{
				return _tableName;
			}
			private set
			{
				if (string.IsNullOrWhiteSpace(value))
					throw new ArgumentException("Value cannot be null or empty.", "name");
				_tableName = value;
			}
		}

		public TablePropertyAttribute(string name)
		{
			this.TableName = name;
		}
	}
}
