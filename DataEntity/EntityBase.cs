using System;
using System.Linq;
using System.Reflection;

namespace BigData.Server.DataAccessHelper.DataEntity
{
	public abstract class EntityBase
	{
		/// <summary>
		/// 根据数据实体特性解析出数据库表信息
		/// </summary>
		/// <returns></returns>
		internal DbTableInfo AnalyzeTableInfo()
		{
			Type entityType = this.GetType();
			TablePropertyAttribute tableProperty = Attribute.GetCustomAttribute(entityType, typeof(TablePropertyAttribute)) as TablePropertyAttribute;
			if (tableProperty == null)
				throw new Exception(string.Format("{0} class need a attribute named 'TableProperty'", entityType.Name));

			DbTableInfo tableInfo = new DbTableInfo() { Name = tableProperty.TableName };
			foreach (PropertyInfo p in entityType.GetProperties()) {
				object[] customAttributes = p.GetCustomAttributes(false);
				FieldPropertyAttribute fpa = customAttributes.OfType<FieldPropertyAttribute>().FirstOrDefault();
				if (fpa == null)
					break;
				DbFieldInfo field = new DbFieldInfo() {
					Name = fpa.FieldName,
					Value = p.GetValue(this, null),
					IsKey = fpa.IsKey,
					IsNullable = fpa.IsNullable
				};

				FieldValueAttribute fva = customAttributes.OfType<FieldValueAttribute>().FirstOrDefault();
				if (fva == null) {
					field.ValueType = FieldValueType.String;
					field.DefaultValue = null;
				}
				else {
					field.ValueType = fva.ValueType;
					field.DefaultValue = fva.DefaultValue;
				}

				IgnoreUpdateAttribute iua = customAttributes.OfType<IgnoreUpdateAttribute>().FirstOrDefault();
				if (iua == null) {
					field.IsIgnoreUpdate = false;
				}
				else {
					field.IsIgnoreUpdate = true;
				}
				tableInfo.Fields.Add(field);
			}
			return tableInfo;
		}

		/// <summary>
		/// 根据特性的字段名找到数据实体类相应的属性名
		/// </summary>
		/// <param name="fieldName"></param>
		/// <returns></returns>
		internal string FindPropertyNameByFieldName(string fieldName)
		{
			string propertyName = string.Empty;
			Type entityType = this.GetType();
			foreach (var p in entityType.GetProperties()) {
				FieldPropertyAttribute fieldProperty = Attribute.GetCustomAttribute(p, typeof(FieldPropertyAttribute)) as FieldPropertyAttribute;
				//FieldPropertyAttribute fieldProperty = p.GetCustomAttributes(false).OfType<FieldPropertyAttribute>().FirstOrDefault();
				if (fieldProperty != null && fieldProperty.FieldName == fieldName) {
					propertyName = p.Name;
					break;
				}
			}
			return propertyName;
		}
	}
}