using MongoDB.Driver.Builders;

namespace BigData.Server.DataAccessHelper.MongoDb
{
    public enum FieldDisplayMode
    {
        Display,
        Hidden
    }

    public class MongoDBFields
    {
        #region 字段属性
        public FieldDisplayMode DisplayMode { get; set; }

        internal FieldsBuilder Builder { get; private set; }
        #endregion

        #region 构造函数
        public MongoDBFields(FieldDisplayMode mode)
        {
            Builder = new FieldsBuilder();
            DisplayMode = mode;
        }
        #endregion

        public MongoDBFields Add(params string[] names)
        {
            if (DisplayMode == FieldDisplayMode.Display)
            {
                Builder.Include(names);
            }
            else if (DisplayMode == FieldDisplayMode.Hidden)
            {
                Builder.Exclude(names);
            }
            return this;
        }

        public override string ToString()
        {
            return Builder.ToBsonDocument().ToString();
        }
    }
}
