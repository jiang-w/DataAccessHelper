using System;
using System.Data;
using System.Xml.Serialization;

namespace BigData.Server.DataAccessHelper.Database
{
	interface IDbProvider
	{
		IDbCommand CreateCommand();
		IDbConnection CreateConnection();
		IDbDataAdapter CreateDataAdapter();
		IDbDataParameter CreateDataParameter();
		string FormatNameForParameter(string parameterName);
		string FormatNameForSql(string parameterName);
		void Initialize();

		[XmlAttribute("allowMultipleActiveDataReaders")]
		bool AllowMARS { get; set; }

		[XmlAttribute("assemblyName")]
		string AssemblyName { get; set; }

		[XmlAttribute("commandBuilderClass")]
		string CommandBuilderClass { get; set; }

		Type CommandBuilderType { get; }

		[XmlAttribute("dataAdapterClass")]
		string DataAdapterClass { get; set; }

		[XmlAttribute("commandClass")]
		string DbCommandClass { get; set; }

		[XmlAttribute("connectionClass")]
		string DbConnectionClass { get; set; }

		[XmlAttribute("description")]
		string Description { get; set; }

		[XmlAttribute("default")]
		bool IsDefault { get; set; }

		[XmlAttribute("enabled")]
		bool IsEnabled { get; set; }

		[XmlIgnore]
		bool IsObdc { get; }

		[XmlAttribute("name")]
		string Name { get; set; }

		Type ParameterDbType { get; }

		[XmlAttribute("parameterDbTypeClass")]
		string ParameterDbTypeClass { get; set; }

		[XmlAttribute("parameterDbTypeProperty")]
		string ParameterDbTypeProperty { get; set; }

		[XmlAttribute("parameterPrefix")]
		string ParameterPrefix { get; set; }

		[XmlAttribute("setDbParameterPrecision")]
		bool SetDbParameterPrecision { get; set; }

		[XmlAttribute("setDbParameterScale")]
		bool SetDbParameterScale { get; set; }

		[XmlAttribute("setDbParameterSize")]
		bool SetDbParameterSize { get; set; }

		[XmlAttribute("useDeriveParameters")]
		bool UseDeriveParameters { get; set; }

		[XmlAttribute("useParameterPrefixInParameter")]
		bool UseParameterPrefixInParameter { get; set; }

		[XmlAttribute("useParameterPrefixInSql")]
		bool UseParameterPrefixInSql { get; set; }

		[XmlAttribute("usePositionalParameters")]
		bool UsePositionalParameters { get; set; }
	}
}
