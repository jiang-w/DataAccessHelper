using System;
using System.Data;
using System.Reflection;
using System.Xml.Serialization;
using System.Xml;
using BigData.Server.DataAccessHelper.Configuration;

namespace BigData.Server.DataAccessHelper.Database
{
	[Serializable, XmlRoot("provider", Namespace = "http://ibatis.apache.org/providers")]
	class DbProvider : IDbProvider
	{
		[NonSerialized]
		private bool _allowMARS;
		[NonSerialized]
		private string _assemblyName = string.Empty;
		[NonSerialized]
		private string _commandBuilderClass = string.Empty;
		[NonSerialized]
		private Type _commandBuilderType;
		[NonSerialized]
		private string _commandClass = string.Empty;
		[NonSerialized]
		private string _connectionClass = string.Empty;
		[NonSerialized]
		private string _dataAdapterClass = string.Empty;
		[NonSerialized]
		private string _description = string.Empty;
		[NonSerialized]
		private bool _isDefault;
		[NonSerialized]
		private bool _isEnabled = true;
		[NonSerialized]
		private string _name = string.Empty;
		[NonSerialized]
		private Type _parameterDbType;
		[NonSerialized]
		private string _parameterDbTypeClass = string.Empty;
		[NonSerialized]
		private string _parameterDbTypeProperty = string.Empty;
		[NonSerialized]
		private string _parameterPrefix = string.Empty;
		[NonSerialized]
		private bool _setDbParameterPrecision = true;
		[NonSerialized]
		private bool _setDbParameterScale = true;
		[NonSerialized]
		private bool _setDbParameterSize = true;
		[NonSerialized]
		private IDbConnection _templateConnection;
		[NonSerialized]
		private bool _templateConnectionIsICloneable;
		[NonSerialized]
		private IDbDataAdapter _templateDataAdapter;
		[NonSerialized]
		private bool _templateDataAdapterIsICloneable;
		[NonSerialized]
		private bool _useDeriveParameters = true;
		[NonSerialized]
		private bool _useParameterPrefixInParameter = true;
		[NonSerialized]
		private bool _useParameterPrefixInSql = true;
		[NonSerialized]
		private bool _usePositionalParameters;
		private const string SQLPARAMETER = "?";

		private void CheckPropertyString(string propertyName, string value)
		{
			if ((value == null) || (value.Trim().Length == 0)) {
				throw new ArgumentException(String.Format("The {0} property cannot be set to a null or empty string value.", propertyName), propertyName);
			}
		}

		private void CheckPropertyType(string propertyName, Type expectedType, Type value)
		{
			if (value == null) {
				throw new ArgumentNullException(propertyName, String.Format("The {0} property cannot be null.", propertyName));
			}
			if (!expectedType.IsAssignableFrom(value)) {
				throw new ArgumentException(String.Format("The Type passed to the {0} property must be an {1} implementation.", propertyName, expectedType.Name));
			}
		}

		public virtual IDbCommand CreateCommand()
		{
			return this._templateConnection.CreateCommand();
		}

		public virtual IDbConnection CreateConnection()
		{
			if (this._templateConnectionIsICloneable) {
				return (IDbConnection)((ICloneable)this._templateConnection).Clone();
			}
			return (IDbConnection)Activator.CreateInstance(this._templateConnection.GetType());
		}

		public virtual IDbDataAdapter CreateDataAdapter()
		{
			if (this._templateDataAdapterIsICloneable) {
				return (IDbDataAdapter)((ICloneable)this._templateDataAdapter).Clone();
			}
			return (IDbDataAdapter)Activator.CreateInstance(this._templateDataAdapter.GetType());
		}

		public virtual IDbDataParameter CreateDataParameter()
		{
			return this._templateConnection.CreateCommand().CreateParameter();
		}

		public override bool Equals(object obj)
		{
			if ((obj == null) || !(obj is IDbProvider)) {
				return false;
			}
			IDbProvider provider = (IDbProvider)obj;
			return (((this._name == provider.Name) && (this._assemblyName == provider.AssemblyName)) && (this._connectionClass == provider.DbConnectionClass));
		}

		public virtual string FormatNameForParameter(string parameterName)
		{
			if (!this._useParameterPrefixInParameter) {
				return parameterName;
			}
			return (this._parameterPrefix + parameterName);
		}

		public virtual string FormatNameForSql(string parameterName)
		{
			if (!this._useParameterPrefixInSql) {
				return "?";
			}
			return (this._parameterPrefix + parameterName);
		}

		public override int GetHashCode()
		{
			return ((this._name.GetHashCode() ^ this._assemblyName.GetHashCode()) ^ this._connectionClass.GetHashCode());
		}

		public void Initialize()
		{
			Assembly assembly = null;
			Type type = null;
			try {
				assembly = Assembly.Load(this._assemblyName);
				type = assembly.GetType(this._dataAdapterClass, true);
				this.CheckPropertyType("DataAdapterClass", typeof(IDbDataAdapter), type);
				this._templateDataAdapter = (IDbDataAdapter)type.GetConstructor(Type.EmptyTypes).Invoke(null);
				type = assembly.GetType(this._connectionClass, true);
				this.CheckPropertyType("DbConnectionClass", typeof(IDbConnection), type);
				this._templateConnection = (IDbConnection)type.GetConstructor(Type.EmptyTypes).Invoke(null);
				this._commandBuilderType = assembly.GetType(this._commandBuilderClass, true);
				this._parameterDbType = assembly.GetType(this._parameterDbTypeClass, true);
				this._templateConnectionIsICloneable = this._templateConnection is ICloneable;
				this._templateDataAdapterIsICloneable = this._templateDataAdapter is ICloneable;
			}
			catch (Exception exception) {
				throw new Exception(string.Format("Could not configure providers. Unable to load provider named \"{0}\" not found, failed. Cause: {1}", this._name, exception.Message), exception);
			}
		}

		public override string ToString()
		{
			return ("Provider " + this._name);
		}

		[XmlAttribute("allowMARS")]
		public bool AllowMARS
		{
			get
			{
				return this._allowMARS;
			}
			set
			{
				this._allowMARS = value;
			}
		}

		[XmlAttribute("assemblyName")]
		public string AssemblyName
		{
			get
			{
				return this._assemblyName;
			}
			set
			{
				this.CheckPropertyString("AssemblyName", value);
				this._assemblyName = value;
			}
		}

		[XmlAttribute("commandBuilderClass")]
		public string CommandBuilderClass
		{
			get
			{
				return this._commandBuilderClass;
			}
			set
			{
				this.CheckPropertyString("CommandBuilderClass", value);
				this._commandBuilderClass = value;
			}
		}

		public Type CommandBuilderType
		{
			get
			{
				return this._commandBuilderType;
			}
		}

		[XmlAttribute("dataAdapterClass")]
		public string DataAdapterClass
		{
			get
			{
				return this._dataAdapterClass;
			}
			set
			{
				this.CheckPropertyString("DataAdapterClass", value);
				this._dataAdapterClass = value;
			}
		}

		[XmlAttribute("commandClass")]
		public string DbCommandClass
		{
			get
			{
				return this._commandClass;
			}
			set
			{
				this.CheckPropertyString("DbCommandClass", value);
				this._commandClass = value;
			}
		}

		[XmlAttribute("connectionClass")]
		public string DbConnectionClass
		{
			get
			{
				return this._connectionClass;
			}
			set
			{
				this.CheckPropertyString("DbConnectionClass", value);
				this._connectionClass = value;
			}
		}

		[XmlAttribute("description")]
		public string Description
		{
			get
			{
				return this._description;
			}
			set
			{
				this._description = value;
			}
		}

		[XmlAttribute("default")]
		public bool IsDefault
		{
			get
			{
				return this._isDefault;
			}
			set
			{
				this._isDefault = value;
			}
		}

		[XmlAttribute("enabled")]
		public bool IsEnabled
		{
			get
			{
				return this._isEnabled;
			}
			set
			{
				this._isEnabled = value;
			}
		}

		[XmlIgnore]
		public bool IsObdc
		{
			get
			{
				return (this._connectionClass.IndexOf(".Odbc.") > 0);
			}
		}

		[XmlAttribute("name")]
		public string Name
		{
			get
			{
				return this._name;
			}
			set
			{
				this.CheckPropertyString("Name", value);
				this._name = value;
			}
		}

		[XmlIgnore]
		public Type ParameterDbType
		{
			get
			{
				return this._parameterDbType;
			}
		}

		[XmlAttribute("parameterDbTypeClass")]
		public string ParameterDbTypeClass
		{
			get
			{
				return this._parameterDbTypeClass;
			}
			set
			{
				this.CheckPropertyString("ParameterDbTypeClass", value);
				this._parameterDbTypeClass = value;
			}
		}

		[XmlAttribute("parameterDbTypeProperty")]
		public string ParameterDbTypeProperty
		{
			get
			{
				return this._parameterDbTypeProperty;
			}
			set
			{
				this.CheckPropertyString("ParameterDbTypeProperty", value);
				this._parameterDbTypeProperty = value;
			}
		}

		[XmlAttribute("parameterPrefix")]
		public string ParameterPrefix
		{
			get
			{
				return this._parameterPrefix;
			}
			set
			{
				if ((value == null) || (value.Length < 1)) {
					this._parameterPrefix = "";
				}
				else {
					this._parameterPrefix = value;
				}
			}
		}

		[XmlAttribute("setDbParameterPrecision")]
		public bool SetDbParameterPrecision
		{
			get
			{
				return this._setDbParameterPrecision;
			}
			set
			{
				this._setDbParameterPrecision = value;
			}
		}

		[XmlAttribute("setDbParameterScale")]
		public bool SetDbParameterScale
		{
			get
			{
				return this._setDbParameterScale;
			}
			set
			{
				this._setDbParameterScale = value;
			}
		}

		[XmlAttribute("setDbParameterSize")]
		public bool SetDbParameterSize
		{
			get
			{
				return this._setDbParameterSize;
			}
			set
			{
				this._setDbParameterSize = value;
			}
		}

		[XmlAttribute("useDeriveParameters")]
		public bool UseDeriveParameters
		{
			get
			{
				return this._useDeriveParameters;
			}
			set
			{
				this._useDeriveParameters = value;
			}
		}

		[XmlAttribute("useParameterPrefixInParameter")]
		public bool UseParameterPrefixInParameter
		{
			get
			{
				return this._useParameterPrefixInParameter;
			}
			set
			{
				this._useParameterPrefixInParameter = value;
			}
		}

		[XmlAttribute("useParameterPrefixInSql")]
		public bool UseParameterPrefixInSql
		{
			get
			{
				return this._useParameterPrefixInSql;
			}
			set
			{
				this._useParameterPrefixInSql = value;
			}
		}

		[XmlAttribute("usePositionalParameters")]
		public bool UsePositionalParameters
		{
			get
			{
				return this._usePositionalParameters;
			}
			set
			{
				this._usePositionalParameters = value;
			}
		}

		#region Static
		private static IDbProvider GetProviderFromProvider(string providerName)
		{
			IDbProvider provider;
			XmlDocument xmlDoc = new XmlDocument();
			xmlDoc.LoadXml(Resource.Providers);
			var xmlnsm = new XmlNamespaceManager(xmlDoc.NameTable);
			xmlnsm.AddNamespace("ns", "http://ibatis.apache.org/providers");
			XmlNode xmlNode = xmlDoc.SelectSingleNode(string.Format("/ns:providers/ns:provider[@name=\"{0}\"]", providerName), xmlnsm);
			if (xmlNode != null)
				provider = ProviderDeserializer.Deserialize(xmlNode);
			else
				provider = null;

			if (provider != null && provider.IsEnabled) {
				provider.Initialize();
			}
			return provider;
		}

		public static IDbProvider DefaultOracleProvider
		{
			get { return GetProviderFromProvider("OracleClient"); }
		}

		public static IDbProvider DefaultSqlServerProvider
		{
			get { return GetProviderFromProvider("SqlServerClient"); }
		}

		public static IDbProvider DefaultMySqlProvider
		{
			get { return GetProviderFromProvider("MySqlClient"); }
		}
		#endregion
	}
}
