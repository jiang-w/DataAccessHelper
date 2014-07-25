
namespace BigData.Server.DataAccessHelper.DataEntity
{
	class DbFieldInfo
	{
		public string Name { get; set; }

		public object Value { get; set; }

		public bool IsNullable { get; set; }

		public bool IsKey { get; set; }

		public FieldValueType ValueType { get; set; }

		public object DefaultValue { get; set; }

		public bool IsIgnoreUpdate { get; set; }
	}
}
