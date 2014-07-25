using System;
using System.Data;
using System.Linq;
using BigData.Server.DataAccessHelper.DataSource;
using BigData.Server.DataAccessHelper.Utility;
using Newtonsoft.Json;

namespace BigData.Server.DataAccessHelper.Index
{
	public class IndexObject
	{
		public long IDX_ID { get; set; }
		public string ENG_SHT { get; set; }
		public string FLD_NAME { get; set; }
		public Int16 IDX_TYP { get; set; }
		public long OBJ_ID { get; set; }
		public IdxParameter[] IDX_PARAM { get; set; }

		public IndexObject(long idxId)
		{
			DataSourceAccess dsAccess = new DataSourceAccess("IDX_LIST");
			dsAccess.Filter = UQuery.Eq("IDX_ID", idxId);
			DataRow dr = dsAccess.Execute().AsEnumerable().FirstOrDefault();
			if (dr != null) {
				InitObj(dr);
			}
			else {
				throw new Exception(string.Format("未找到指定的指标(IDX_ID：{0})", idxId));
			}
		}

		private void InitObj(DataRow dr)
		{
			this.IDX_ID = Convert.ToInt64(dr["IDX_ID"]);
			this.ENG_SHT = dr["ENG_SHT"].ToString();
			this.FLD_NAME = dr["FLD_NAME"].ToString();
			this.IDX_TYP = Convert.ToInt16(dr["IDX_TYP"]);
			this.OBJ_ID = Convert.ToInt64(dr["OBJ_ID"]);
			this.IDX_PARAM = JsonConvert.DeserializeObject<IdxParameter[]>(dr["IDX_PARAM"].ToString()) ?? new IdxParameter[0];
		}
	}
}
