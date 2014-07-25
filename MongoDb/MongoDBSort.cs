using BigData.Server.DataAccessHelper.Utility;
using MongoDB.Driver.Builders;

namespace BigData.Server.DataAccessHelper.MongoDb
{
    public class MongoDBSort
    {
        #region 字段属性
        internal SortByBuilder Builder { get; private set; }
        #endregion

        #region 构造函数
        public MongoDBSort()
        {
            Builder = new SortByBuilder();
        }
        #endregion

        public MongoDBSort Add(string key, SortMode mode)
        {
            if (mode == SortMode.ASC)
            {
                Builder.Ascending(key);
            }
            else if (mode == SortMode.DESC)
            {
                Builder.Descending(key);
            }
            return this;
        }

        public override string ToString()
        {
            return Builder.ToBsonDocument().ToString();
        }
    }
}
