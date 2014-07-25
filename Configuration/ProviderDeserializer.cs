using System.Collections.Specialized;
using System.Xml;
using BigData.Server.DataAccessHelper.Database;

namespace BigData.Server.DataAccessHelper.Configuration
{
	sealed class ProviderDeserializer
	{
		public static IDbProvider Deserialize(XmlNode node)
		{
			IDbProvider provider = new DbProvider();
			NameValueCollection attributes = ParseAttributes(node);
			provider.AssemblyName = attributes["assemblyName"];
			provider.CommandBuilderClass = attributes["commandBuilderClass"];
			provider.DbCommandClass = attributes["commandClass"];
			provider.DbConnectionClass = attributes["connectionClass"];
			provider.DataAdapterClass = attributes["dataAdapterClass"];
			provider.Description = attributes["description"];
			provider.IsDefault = GetBooleanAttribute(attributes, "default", false);
			provider.IsEnabled = GetBooleanAttribute(attributes, "enabled", true);
			provider.Name = attributes["name"];
			provider.ParameterDbTypeClass = attributes["parameterDbTypeClass"];
			provider.ParameterDbTypeProperty = attributes["parameterDbTypeProperty"];
			provider.ParameterPrefix = attributes["parameterPrefix"];
			provider.SetDbParameterPrecision = GetBooleanAttribute(attributes, "setDbParameterPrecision", true);
			provider.SetDbParameterScale = GetBooleanAttribute(attributes, "setDbParameterScale", true);
			provider.SetDbParameterSize = GetBooleanAttribute(attributes, "setDbParameterSize", true);
			provider.UseDeriveParameters = GetBooleanAttribute(attributes, "useDeriveParameters", true);
			provider.UseParameterPrefixInParameter = GetBooleanAttribute(attributes, "useParameterPrefixInParameter", true);
			provider.UseParameterPrefixInSql = GetBooleanAttribute(attributes, "useParameterPrefixInSql", true);
			provider.UsePositionalParameters = GetBooleanAttribute(attributes, "usePositionalParameters", false);
			provider.AllowMARS = GetBooleanAttribute(attributes, "allowMARS", false);
			return provider;
		}

		private static NameValueCollection ParseAttributes(XmlNode node)
		{
			NameValueCollection values = new NameValueCollection();
			int count = node.Attributes.Count;
			for (int i = 0; i < count; i++) {
				XmlAttribute attribute = node.Attributes[i];
				values.Add(attribute.Name, attribute.Value);
			}
			return values;
		}

		private static bool GetBooleanAttribute(NameValueCollection attributes, string name, bool def)
		{
			string s = attributes[name];
			if (s == null) {
				return def;
			}
			return XmlConvert.ToBoolean(s);
		}
	}
}
