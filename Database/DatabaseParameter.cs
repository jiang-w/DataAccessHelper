using System;
using System.Data;

namespace BigData.Server.DataAccessHelper.Database
{
	public class DatabaseParameter
	{
		public DbType DbType { get; set; }
		public ParameterDirection Direction { get; set; }
		public string ParameterName { get; set; }
		public object Value { get; set; }

		public DatabaseParameter(string parameterName, object value)
		{
			this.ParameterName = parameterName;
			this.Value = value ?? DBNull.Value;
			this.Direction = ParameterDirection.Input;

			switch (Type.GetTypeCode(value.GetType())) {
				case TypeCode.Boolean:
					this.DbType = DbType.Boolean;
					break;
				case TypeCode.Byte:
					this.DbType = DbType.Byte;
					break;
				case TypeCode.DateTime:
					this.DbType = DbType.DateTime;
					break;
				case TypeCode.Decimal:
					this.DbType = DbType.Decimal;
					break;
				case TypeCode.Double:
					this.DbType = DbType.Double;
					break;
				case TypeCode.Int16:
					this.DbType = DbType.Int16;
					break;
				case TypeCode.Int32:
					this.DbType = DbType.Int32;
					break;
				case TypeCode.Int64:
					this.DbType = DbType.Int64;
					break;
				case TypeCode.Object:
					throw new ArgumentException(string.Format("DBParameter不支持{0}对象类型", value.GetType().FullName), "value");
				case TypeCode.SByte:
					this.DbType = DbType.SByte;
					break;
				case TypeCode.Single:
					this.DbType = DbType.Single;
					break;
				case TypeCode.UInt16:
					this.DbType = DbType.UInt16;
					break;
				case TypeCode.UInt32:
					this.DbType = DbType.UInt32;
					break;
				case TypeCode.UInt64:
					this.DbType = DbType.UInt64;
					break;
				default:
					this.DbType = DbType.String;
					break;
			}
		}
	}
}
