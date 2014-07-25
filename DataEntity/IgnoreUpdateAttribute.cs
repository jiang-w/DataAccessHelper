using System;

namespace BigData.Server.DataAccessHelper.DataEntity
{
	/// <summary>
	/// 字段特性，表示此字段忽略更新
	/// </summary>
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
	public class IgnoreUpdateAttribute : Attribute
	{
	}
}
