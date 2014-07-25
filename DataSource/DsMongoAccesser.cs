using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using BigData.Server.DataAccessHelper.MongoDb;

namespace BigData.Server.DataAccessHelper.DataSource
{
    class DsMongoAccesser : DsAccesser
    {
        /// <summary>
        /// MongoDB连接
        /// </summary>
        public MongoDBConnection Connection { get; set; }

        /// <summary>
        /// 数据源在MongoDB中的数据库名
        /// </summary>
        public string Database { get { return base.DsObj.Eng_Name; } }

        /// <summary>
        /// 待访问的MongoDB集合名列表
        /// </summary>
        public List<string> CollectionList { get { return GetCollectionNameList(); } }

        public DsMongoAccesser(DataSourceObject dsObj)
        {
            this.Connection = MongoDBConnection.Default;
            base.DsObj = dsObj;
        }

        public override DataTable Execute()
        {
            DataTable returnTable = null;
            if (this.CollectionList.Count == 1) {
                MongoDBAccess mongoAccess = new MongoDBAccess(this.Connection.Host, this.Connection.Port, this.Database, this.CollectionList.First());
                if (this.DsObj.Ds_Type == 7)    //2013-1-18修改   增加对增量更新数据源MongoDB的访问
                {
                    //兼容针对老的静态分段数据源的视图配置，将传递的数据源参数添加到过滤参数中
                    mongoAccess.Filter = MongoDBFilter.And(this.DsObj.InnerParams.Where(p => p.Value != null).Select(p =>
                     p.Value is Array ? MongoDBFilter.In(p.Name, (Array)p.Value) : MongoDBFilter.Eq(p.Name, p.Value)).ToArray());
                    if (base.Filter != null)
                        mongoAccess.Filter = MongoDBFilter.And(mongoAccess.Filter, base.Filter.ConvertToMongoDBFilter());
                }
                else {
                    mongoAccess.Filter = base.Filter != null ? base.Filter.ConvertToMongoDBFilter() : null;
                }

                /*2012-11-19 由于MongoDB不支持中文字符拼音排序，所以通过DataTable进行排序
                mongoAccess.Order = Order != null ? Order.ConvertToMongoDBOrder() : null;
                */
                mongoAccess.Fields = base.Fields != null && base.Fields.Length > 0 ? new MongoDBFields(FieldDisplayMode.Display).Add(base.Fields) : null;
                returnTable = mongoAccess.Execute();
            }
            else {
                foreach (string colname in CollectionList) {
                    MongoDBAccess mongoAccess = new MongoDBAccess(this.Connection.Host, this.Connection.Port, this.Database, colname);
                    if (mongoAccess.CollectionExists()) {
                        mongoAccess.Filter = base.Filter != null ? base.Filter.ConvertToMongoDBFilter() : null;
                        mongoAccess.Fields = base.Fields != null && base.Fields.Length > 0 ? new MongoDBFields(FieldDisplayMode.Display).Add(base.Fields) : null;
                        if (returnTable == null)
                            returnTable = mongoAccess.Execute();
                        else
                            returnTable.Merge(mongoAccess.Execute());
                    }
                }
                if (returnTable == null) {
                    throw new Exception(string.Format("MongoDB找不到指定的集合{2}（服务器：{0}，数据库：{1}）"
                        , this.Connection.ConnectionString, this.Database, string.Join(",", this.CollectionList)));
                }
            }

            if (base.Sort != null && base.Sort.Length > 0)//2012-11-19 修改
			{
                DataView dv = returnTable.DefaultView;
                dv.Sort = string.Join(",", base.Sort.Select(sort => sort.ToString()));
                returnTable = dv.ToTable();
            }

            return returnTable;
        }

        public override DataTable Execute(int pageSize, int pageIndex, out int pageCount, out int recordCount)
        {
            DataTable returnTable = null;
            if (CollectionList.Count == 1) {
                MongoDBAccess mongoAccess = new MongoDBAccess(this.Connection.Host, this.Connection.Port, this.Database, CollectionList.First());
                mongoAccess.Filter = base.Filter != null ? base.Filter.ConvertToMongoDBFilter() : null;
                if (base.Sort != null) {
                    MongoDBSort mongoSort = new MongoDBSort();
                    foreach (var s in base.Sort) {
                        mongoSort.Add(s.FieldName, s.SortMode);
                    }
                    mongoAccess.Sort = mongoSort;
                }
                mongoAccess.Fields = base.Fields != null && base.Fields.Length > 0 ? new MongoDBFields(FieldDisplayMode.Display).Add(base.Fields) : null;
                returnTable = mongoAccess.Execute(pageSize, pageIndex, out pageCount, out recordCount);
            }
            else {
                returnTable = Execute();
                recordCount = returnTable.Rows.Count;
                pageSize = pageSize > recordCount || pageSize <= 0 ? recordCount : pageSize;
                pageCount = pageSize != 0 ? Convert.ToInt32(Math.Ceiling(recordCount * 1.0 / pageSize)) : 0;
                pageIndex = pageIndex <= 0 ? 1 : pageIndex > pageCount ? pageCount : pageIndex;

                if (pageSize != recordCount) {
                    int startIndex = (pageIndex - 1) * pageSize;
                    int endIndex = pageIndex * pageSize - 1;
                    DataTable newdt = returnTable.Clone();
                    for (int i = startIndex; i <= endIndex; i++) {
                        newdt.ImportRow(returnTable.Rows[i]);
                    }
                    returnTable.Dispose();
                    returnTable = newdt;
                }
            }

            return returnTable;
        }

        /// <summary>
        /// 生成MongoDB集合名列表
        /// </summary>
        /// <returns></returns>
        private List<string> GetCollectionNameList()
        {
            List<string> collectionNameList = new List<string>();
            if (base.DsObj.InnerParams.Length > 0 && base.DsObj.Ds_Type != 7) {
                List<DsParameter> innerParamList = base.DsObj.InnerParams.ToList();
                innerParamList.Sort(new Comparison<DsParameter>(
                    delegate(DsParameter param1, DsParameter param2) { return param1.Name.CompareTo(param2.Name); }
                    ));
                innerParamList.ForEach(param =>
                {
                    List<object> paramValues = new List<object>();
                    if (param.Value is Array)
                        paramValues.AddRange((object[])param.Value);
                    else
                        paramValues.Add(param.Value);

                    if (collectionNameList.Count == 0) {
                        collectionNameList.AddRange(paramValues.Select(item => ParamValueFormat(item)));
                    }
                    else {
                        List<string> tempList = new List<string>();
                        foreach (string n in collectionNameList) {
                            tempList.AddRange(paramValues.Select(item => n + "_" + ParamValueFormat(item)));
                        }
                        collectionNameList = tempList;
                    }
                });
            }
            else {
                collectionNameList.Add(base.DsObj.Eng_Name);
            }
            return collectionNameList;
        }

        private string ParamValueFormat(object paramValue)
        {
            if (paramValue is DateTime)
                return ((DateTime)paramValue).ToString("yyyy-MM-dd");
            else
                return paramValue.ToString();
        }
    }
}
