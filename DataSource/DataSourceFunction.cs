using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using BigData.Server.DataAccessHelper.Database;
using BigData.Server.DataAccessHelper.Utility;

namespace BigData.Server.DataAccessHelper.DataSource
{
	public class DataSourceFunction
	{
		#region static
		private static readonly Dictionary<string, string> functionDic;

		static DataSourceFunction()
		{
			functionDic = new Dictionary<string, string>();
			functionDic.Add("FUNC_TRD_DT", "FUNC_TRD_DT");//提供给终端取交易日数据
			functionDic.Add("FUNC_YEAR", "FUNC_YEAR");//获取年份
			functionDic.Add("FUNC_COM_RPT_SN", "ComRptDateSNByRptDate");//公司财务报告期序列(取某个报告期前推或后推N个报告期序列)
			functionDic.Add("FUNC_COM_RPT_DT", "FUNC_COM_RPT_DT");//公司财务报告期(根据SEUC_ID取某个报告期前推或后推N个报告期)
			functionDic.Add("FUNC_FUND_RPT_DT", "FUNC_FUND_RPT_DT");//基金报告期(取某个日期前推或后推N个报告期)
			functionDic.Add("FUNC_FUND_RPT_TYP", "FUNC_FUND_RPT_TYP");//根据某个日期获取基金最新报告期类型（全部）
			functionDic.Add("FUNC_F9_COM_RPT_SN", "RptDateSNBySecuId");//根据SEUC_ID取N个公司报告期序列
			functionDic.Add("FUNC_F9_FUND_RPT_SN", "RptDateSNBySecuId");//根据SEUC_ID取N个基金报告期序列
			functionDic.Add("FUNC_CN_BND_MAX_RPT_DT", "FUNC_CN_BND_MAX_RPT_DT");//中债登最新报告期
			functionDic.Add("FUNC_MAX_TABLE_DT", "FUNC_MAX_TABLE_DT");//获取数据库指定表的某个日期字段的最大值

			functionDic.Add("FUNC_RPT_SN_BY_COMID", "RptDateSNByComId");//报告期序列(根据COM_ID返回最新报告期前推或后推N个报告期的序列,n=-1是取最新报告期)
		}
		#endregion

		private DatabaseConnection _conn;

		private struct ReportDate
		{
			public int Type { get; set; }
			public DateTime Date { get; set; }
		}

		public DataSourceFunction()
		{
			_conn = DatabaseConnection.Default;
		}

		public string ScalarFunction(string expression)
		{
			expression = expression.Trim();
			while (IsFunction(expression)) {
				MatchCollection matches = new Regex(@"\w+\([^()]*\)").Matches(expression);
				foreach (Match m in matches) {
					string subFunction = m.Value;
					if (subFunction == expression) {
						string functionName = new Regex(@"^\w+\(").Match(subFunction).Value.TrimEnd('(').ToUpper();
						string paramString = new Regex(@"\(.*\)$").Match(expression).Value;
						paramString = paramString.Substring(1, paramString.Length - 2).Trim();
						string[] functionParams = paramString.Split(',');

						if (functionDic.ContainsKey(functionName)) {
							try {
								MethodInfo methodInfo = this.GetType().GetMethod(functionDic[functionName], BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
								object resultObject = methodInfo.Invoke(this, new object[] { functionParams });
								expression = resultObject.ToString();
							}
							catch {
								throw new Exception(string.Format("数据源函数{0}执行出错", subFunction));
							}
						}
						else {
							throw new Exception(string.Format("数据源函数{0}未定义", functionName));
						}
					}
					else {
						string funcValue = ScalarFunction(subFunction);
						funcValue = FilterFormat(funcValue);
						expression = expression.Replace(subFunction, funcValue);
					}
				}
			}
			return expression;
		}

		private string FilterFormat(string value)
		{
			if (new Regex(@"date'\d{4}-\d{1,2}-\d{1,2}'").IsMatch(value))//日期型字符串date'2011-01-30'
            {
				value = new Regex(@"\d{4}-\d{1,2}-\d{1,2}").Match(value).Value;
			}
			return value;
		}

		public bool IsFunction(string expression)
		{
			Regex regex = new Regex(@"^\s*\w+\(.*\)\s*$");
			if (regex.IsMatch(expression)) {
				return true;
			}
			else {
				return false;
			}
		}

		#region Function

		#region 交易日函数
		/// <summary>
		/// 取交易日函数
		/// </summary>
		/// <param name="vals">
		/// typ--函数类型标识
		/// date--某个给定的日期，默认为系统当前日期
		/// n--前推或者后推N个交易日
		/// </param>
		/// <returns></returns>
		private string FUNC_TRD_DT(params string[] vals)
		{
			int typ = vals.Length > 0 && vals[0] != string.Empty ? int.Parse(vals[0]) : 0;
			string date = vals.Length > 1 && vals[1] != string.Empty ? vals[1] : DateTime.Now.ToString("yyyy-MM-dd");
			int n = vals.Length > 2 && vals[2] != string.Empty ? int.Parse(vals[2]) : 0;

			string returnValue = string.Empty;
			switch (typ) {
				case 1://交易日计算  返回某天（默认为当天）前推或后推N个交易日
                    {
						returnValue = string.Format("date'{0}'", AddTrdDate(Convert.ToDateTime(date), n).ToString("yyyy-MM-dd"));
						break;
					}
				case 2://最新交易日  返回小于等于某天（默认为当天）的最新一个交易日日期
                    {
						DateTime currentDate = Convert.ToDateTime(date);
						if (IsTrdDate(currentDate))
							returnValue = string.Format("date'{0}'", currentDate.ToString("yyyy-MM-dd"));
						else
							returnValue = string.Format("date'{0}'", AddTrdDate(currentDate, -1).ToString("yyyy-MM-dd"));
						break;
					}
				case 3://上一交易日  返回小于某天（默认为当天）的最新一个交易日期
                    {
						returnValue = string.Format("date'{0}'", AddTrdDate(Convert.ToDateTime(date), -1).ToString("yyyy-MM-dd"));
						break;
					}
				case 4://下一交易日  返回大于某天（默认为当天）的最新一个交易日期
                    {
						returnValue = string.Format("date'{0}'", AddTrdDate(Convert.ToDateTime(date), 1).ToString("yyyy-MM-dd"));
						break;
					}
				case 5://最近交易日  返回大于等于某天（默认为当天）的最新一个交易日期 
                    {
						DateTime currentDate = Convert.ToDateTime(date);
						if (IsTrdDate(currentDate))
							returnValue = string.Format("date'{0}'", currentDate.ToString("yyyy-MM-dd"));
						else
							returnValue = string.Format("date'{0}'", AddTrdDate(currentDate, 1).ToString("yyyy-MM-dd"));
						break;
					}
				case 6://N年前
                    {
						DateTime tmpDate = Convert.ToDateTime(date).AddYears(n * -1);
						return FUNC_TRD_DT("5", tmpDate.ToString("yyyy-MM-dd"));
					}
				case 7://N周前
                    {
						DateTime tmpDate = Convert.ToDateTime(date).AddDays(n * -7);
						return FUNC_TRD_DT("5", tmpDate.ToString("yyyy-MM-dd"));
					}
				case 8://N季前
                    {
						DateTime tmpDate = Convert.ToDateTime(date).AddMonths(n * -3);
						return FUNC_TRD_DT("5", tmpDate.ToString("yyyy-MM-dd"));
					}
				case 9://N月前 
                    {
						DateTime tmpDate = Convert.ToDateTime(date).AddMonths(n * -1);
						return FUNC_TRD_DT("5", tmpDate.ToString("yyyy-MM-dd"));
					}
				case 10://年初 
                    {
						int year = Convert.ToDateTime(date).Year;
						DateTime tmpDate = new DateTime(year, 1, 1);
						return FUNC_TRD_DT("5", tmpDate.ToString("yyyy-MM-dd"));
					}
				case 11://月初 
                    {
						int year = Convert.ToDateTime(date).Year;
						int month = Convert.ToDateTime(date).Month;
						DateTime tmpDate = new DateTime(year, month, 1);
						return FUNC_TRD_DT("5", tmpDate.ToString("yyyy-MM-dd"));
					}
				case 12://周初 
                    {
						DateTime d = Convert.ToDateTime(date);
						DayOfWeek week = d.DayOfWeek;
						int dayNum = week == DayOfWeek.Sunday ? 6 : (int)week - 1;
						DateTime tmpDate = d.AddDays(-1 * dayNum);
						return FUNC_TRD_DT("5", tmpDate.ToString("yyyy-MM-dd"));
					}
				case 13://季初 
                    {
						int year = Convert.ToDateTime(date).Year;
						int month = Convert.ToDateTime(date).Month;
						DateTime tmpDate = new DateTime(year, month / 3 * 3 + 1, 1);
						return FUNC_TRD_DT("5", tmpDate.ToString("yyyy-MM-dd"));
					}
			}
			return returnValue;
		}
		#endregion

		#region 年份函数
		/// <summary>
		/// 获取年份
		/// </summary>
		/// <param name="vals"></param>
		/// typ--函数类型标识
		/// n--前推或者后推N个年
		/// <returns></returns>
		private string FUNC_YEAR(params string[] vals)
		{
			string resultString = string.Empty;
			int typ = vals.Length > 0 && vals[0] != string.Empty ? int.Parse(vals[0]) : 0;
			int n = vals.Length > 1 && vals[1] != string.Empty ? int.Parse(vals[1]) : 0;
			switch (typ) {
				case 1:
					resultString = DateTime.Now.Year.ToString();
					break;
				case 2:
					resultString = DateTime.Now.AddYears(n).Year.ToString();
					break;
			}
			return resultString;
		}
		#endregion

		#region 报告期函数

		/// <summary>
		/// 取公司报告期函数
		/// </summary>
		/// <param name="vals">
		/// typ--取报告期的类型（1：最新，2：年报，3：中报或年报）
		/// secuId
		/// date--某个报告期
		/// n--前推(小于0)后推(大于0)N
		/// </param>
		/// <returns></returns>
		private string FUNC_COM_RPT_DT(params string[] vals)
		{
			int typ = vals.Length > 0 && vals[0] != string.Empty ? int.Parse(vals[0]) : 0;
			int secuId = vals.Length > 1 && vals[1] != string.Empty ? int.Parse(vals[1]) : 0;
			string date = vals.Length > 2 && vals[2] != string.Empty ? vals[2] : DateTime.Now.ToString("yyyy-MM-dd");
			int n = vals.Length > 3 ? int.Parse(vals[3]) : 0;

			string returnValue = string.Empty;
			switch (typ) {
				case 1://最新报告期          返回所输入证券的最新报告期 
                    {
						return RptDateSNBySecuId("1", secuId.ToString(), "-1");
					}
				case 2://最新年报            返回所输入证券的最新年报报告期
                    {
						return RptDateSNBySecuId("2", secuId.ToString(), "-1");
					}
				case 3://最新中报或年报      返回所输入证券的最新中报或年报报告期  
                    {
						return RptDateSNBySecuId("3", secuId.ToString(), "-1");
					}
				case 4://报告期计算          返回某个报告期前推或后推N个报告期
                    {
						string rptDtSN = ComRptDateSNByRptDate("1", date, n.ToString());
						string[] rptDts = rptDtSN.Split(',');
						return rptDts.LastOrDefault();
					}
				case 5://年报计算            返回某个报告期前推或后推N个年报  
                    {
						string rptDtSN = ComRptDateSNByRptDate("2", date, n.ToString());
						string[] rptDts = rptDtSN.Split(',');
						return rptDts.LastOrDefault();
					}
				case 6://中报或年报          返回某个报告期前推或后推N个中报或年报  
                    {
						string rptDtSN = ComRptDateSNByRptDate("3", date, n.ToString());
						string[] rptDts = rptDtSN.Split(',');
						return rptDts.LastOrDefault();
					}
				case 7://系统最新报告期      按照当前系统时间返回相应的最新报告期
                    {
						ReportDate newsetDate = COM_RPT_DT_NEWEST(DateTime.Now);
						returnValue = string.Format("date'{0}'", newsetDate.Date.ToString("yyyy-MM-dd"));
						break;
					}
				case 8://系统最新年报        按照当前系统时间返回相应的最新年报报告期
                    {
						ReportDate newsetDate = COM_RPT_DT_NEWEST(DateTime.Now, new int[] { 12 });
						returnValue = string.Format("date'{0}'", newsetDate.Date.ToString("yyyy-MM-dd"));
						break;
					}
				case 9://系统最新中报或年报  按照当前系统时间返回相应的最新中报或年报报告期 
                    {
						ReportDate newsetDate = COM_RPT_DT_NEWEST(DateTime.Now, new int[] { 6, 12 });
						returnValue = string.Format("date'{0}'", newsetDate.Date.ToString("yyyy-MM-dd"));
						break;
					}
			}
			return returnValue;
		}

		/// <summary>
		/// 取基金报告期
		/// </summary>
		/// <param name="vals">
		/// typ--函数类型标识
		/// date--某个日期，默认为当前日期
		/// n--前推(小于0)后推(大于0)N
		/// rptTyp--报告期类型（1, 2, 3, 4, 6, 12）
		/// </param>
		/// <returns></returns>
		private string FUNC_FUND_RPT_DT(params string[] vals)
		{
			DateTime returnDate = DateTime.Now;
			int typ = vals.Length > 0 && vals[0] != string.Empty ? int.Parse(vals[0]) : 0;
			string date = vals.Length > 1 && vals[1] != string.Empty ? vals[1] : DateTime.Now.ToString("yyyy-MM-dd");
			int n = vals.Length > 2 && vals[2] != string.Empty ? int.Parse(vals[2]) : 0;
			int[] rptTypList = vals.Length > 3 && vals[3] != string.Empty ?
				new int[] { int.Parse(vals[3]) } : new int[] { 1, 2, 3, 4, 6, 12 };

			switch (typ) {
				case 1://系统最新报告期（四）:一季报、中报、三季报、年报
                    {
						rptTypList = new int[] { 1, 6, 3, 12 };
						ReportDate newestRptDt = FUND_RPT_DT_NEWEST(DateTime.Now, rptTypList);
						returnDate = newestRptDt.Date;
						break;
					}
				case 2://系统最新年报（四）
                    {
						rptTypList = new int[] { 12 };
						ReportDate newestRptDt = FUND_RPT_DT_NEWEST(DateTime.Now, rptTypList);
						returnDate = newestRptDt.Date;
						break;
					}
				case 3://系统最新中报或年报（四）
                    {
						rptTypList = new int[] { 6, 12 };
						ReportDate newestRptDt = FUND_RPT_DT_NEWEST(DateTime.Now, rptTypList);
						returnDate = newestRptDt.Date;
						break;
					}
				case 4://系统最新报告期（季）
                    {
						rptTypList = new int[] { 1, 2, 3, 4 };
						ReportDate newestRptDt = FUND_RPT_DT_NEWEST(DateTime.Now, rptTypList);
						returnDate = newestRptDt.Date;
						break;
					}
				case 5://报告期计算（六） 根据某个日期来判断最新报告期并返回前推或者后退n个报告期
                    {
						ReportDate newestRptDt = FUND_RPT_DT_NEWEST(Convert.ToDateTime(date), rptTypList);
						List<ReportDate> rptDtList = FUND_RPT_DT_LIST(newestRptDt, new int[] { 1, 2, 3, 4, 6, 12 }, n);
						returnDate = rptDtList.Last().Date;
						break;
					}
				case 6://报告期序列（六） 根据某个日期来判断最新报告期并返回前推或者后退n个报告期序列
                    {
						ReportDate newestRptDt = FUND_RPT_DT_NEWEST(Convert.ToDateTime(date), rptTypList);
						List<ReportDate> rptDtList = FUND_RPT_DT_LIST(newestRptDt, new int[] { 1, 2, 3, 4, 6, 12 }, n);
						string returnValue = string.Empty;
						foreach (ReportDate rpt_dt in rptDtList) {
							returnValue += returnValue != string.Empty ? "," + string.Format("date'{0}'", rpt_dt.Date.ToString("yyyy-MM-dd"))
								: string.Format("date'{0}'", rpt_dt.Date.ToString("yyyy-MM-dd"));
						}
						return returnValue;
					}
				case 7://系统最新报告期（六）
                    {
						ReportDate newestRptDt = FUND_RPT_DT_NEWEST(DateTime.Now, rptTypList);
						returnDate = newestRptDt.Date;
						break;
					}
				case 8://系统最新年报（六）
                    {
						rptTypList = new int[] { 12 };
						ReportDate newestRptDt = FUND_RPT_DT_NEWEST(DateTime.Now, rptTypList);
						returnDate = newestRptDt.Date;
						break;
					}
				case 9://系统最新中报或年报（六）
                    {
						rptTypList = new int[] { 6, 12 };
						ReportDate newestRptDt = FUND_RPT_DT_NEWEST(DateTime.Now, rptTypList);
						returnDate = newestRptDt.Date;
						break;
					}
				case 10://报告期序列（季） 根据某个日期来判断最新报告期并返回前推或者后退n个报告期序列
                    {
						rptTypList = new int[] { 1, 2, 3, 4 };
						ReportDate newestRptDt = FUND_RPT_DT_NEWEST(Convert.ToDateTime(date), rptTypList);
						List<ReportDate> rptDtList = FUND_RPT_DT_LIST(newestRptDt, rptTypList, n);
						string returnValue = string.Empty;
						foreach (ReportDate rpt_dt in rptDtList) {
							returnValue += returnValue != string.Empty ? "," + string.Format("date'{0}'", rpt_dt.Date.ToString("yyyy-MM-dd"))
								: string.Format("date'{0}'", rpt_dt.Date.ToString("yyyy-MM-dd"));
						}
						return returnValue;
					}
			}
			return string.Format("date'{0}'", returnDate.ToString("yyyy-MM-dd"));
		}

		/// <summary>
		/// 中债登最新报告期
		/// </summary>
		/// <param name="vals">无参数</param>
		/// <returns>返回日期格式yyyyMM</returns>
		private string FUNC_CN_BND_MAX_RPT_DT(params string[] vals)
		{
			string sql = "SELECT MAX(END_DT) MAXDATE FROM BNDCN_CB_M_DPST_TYP";
			DatabaseAccess dbAccess = new DatabaseAccess(_conn);
			DateTime maxDate = (DateTime)dbAccess.ExcuteScalar(sql);
			return maxDate.ToString("yyyyMM");
		}

		/// <summary>
		/// 获取数据库指定表的某个日期字段的最大值
		/// </summary>
		/// <param name="vals">
		/// tableName：表名
		/// fieldName：字段名
		/// </param>
		/// <returns></returns>
		private string FUNC_MAX_TABLE_DT(params string[] vals)
		{
			string tableName = vals.Length > 0 ? vals[0] : string.Empty;
			string fieldName = vals.Length > 1 ? vals[1] : string.Empty;
			string sql = string.Format("SELECT MAX({1}) MAXDATE FROM {0}", tableName, fieldName);
			DatabaseAccess dbAccess = new DatabaseAccess(_conn);
			DateTime maxDate = (DateTime)dbAccess.ExcuteScalar(sql);
			return string.Format("date'{0}'", maxDate.Date.ToString("yyyy-MM-dd"));
		}

		#endregion

		#region 返回报告期序列函数

		/// <summary>
		/// 个股报告期序列(根据某个报告期前推或后推N个报告期的序列)
		/// </summary>
		/// <param name="vals">
		/// typ--取报告期的类型（1：最新，2：年报，3：中报或年报，4：季报）
		/// date--某个报告期
		/// n--前推(小于0)后推(大于0)N
		/// </param>
		/// <returns></returns>
		private string ComRptDateSNByRptDate(params string[] vals)
		{
			string returnValue = string.Empty;
			int typ = vals.Length > 0 && vals[0] != string.Empty ? int.Parse(vals[0]) : 0;
			string date = vals.Length > 1 && vals[1] != string.Empty ? vals[1] : DateTime.Now.ToString("yyyy-MM-dd");//某个报告期
			int n = vals.Length > 2 && vals[2] != string.Empty ? int.Parse(vals[2]) : 0;

			#region 报告期取值类型列表
			int[] typeList;
			switch (typ) {
				case 1: typeList = new int[] { 1, 6, 9, 12 };
					break;
				case 2: typeList = new int[] { 12 };
					break;
				case 3: typeList = new int[] { 6, 12 };
					break;
				case 4: typeList = new int[] { 1, 9 };
					break;
				default: typeList = new int[] { 1, 6, 9, 12 };
					break;
			}
			#endregion

			List<KeyValuePair<int, string>> rptDateList = new List<KeyValuePair<int, string>>();
			rptDateList.Add(new KeyValuePair<int, string>(1, "3-31"));
			rptDateList.Add(new KeyValuePair<int, string>(6, "6-30"));
			rptDateList.Add(new KeyValuePair<int, string>(9, "9-30"));
			rptDateList.Add(new KeyValuePair<int, string>(12, "12-31"));

			KeyValuePair<int, string> currentKeyValue = rptDateList.Where(item =>
				item.Value == Convert.ToDateTime(date).ToString("M-dd")).First();
			ReportDate rptDt = new ReportDate { Date = Convert.ToDateTime(date), Type = currentKeyValue.Key };

			List<ReportDate> rptDtList = COM_RPT_DT_LIST(rptDt, typeList, n);
			foreach (ReportDate rpt_dt in rptDtList) {
				returnValue += returnValue != string.Empty ? "," + string.Format("date'{0}'", rpt_dt.Date.ToString("yyyy-MM-dd"))
					: string.Format("date'{0}'", rpt_dt.Date.ToString("yyyy-MM-dd"));
			}
			return returnValue;
		}

		/// <summary>
		/// 报告期序列(根据SECU_ID返回最新报告期前推或后推N个报告期的序列)
		/// </summary>
		/// <param name="vals"></param>
		/// typ--取报告期的类型（1：最新，2：年报，3：中报或年报，4：季报）
		/// secuId--包括股票、基金、债券
		/// n--前推(小于0)后推(大于0)N
		/// <returns></returns>
		private string RptDateSNBySecuId(params string[] vals)
		{
			int typ = vals.Length > 0 && vals[0] != string.Empty ? int.Parse(vals[0]) : 0;
			int secuId = vals.Length > 1 && vals[1] != string.Empty ? int.Parse(vals[1]) : 0;
			int n = vals.Length > 2 && vals[2] != string.Empty ? int.Parse(vals[2]) : 0;

			string returnValue = string.Empty;
			DataSourceAccess dsAccess = new DataSourceAccess("666");
			dsAccess.Filter = UQuery.Eq("SECU_ID", secuId);
			DataTable table = dsAccess.Execute();

			if (table.Rows.Count > 0 && n != 0) {
				DateTime newRptDt = Convert.ToDateTime(table.Rows[0]["END_DT"]);
				int typCode = Convert.ToInt32(table.Rows[0]["TYP_CODEI"]);
				if (typCode == 11)//基金
                {
					#region 报告期取值类型列表
					int[] typeList;
					switch (typ) {
						case 1: typeList = new int[] { 1, 6, 3, 12 };
							break;
						case 2: typeList = new int[] { 12 };
							break;
						case 3: typeList = new int[] { 6, 12 };
							break;
						case 4: typeList = new int[] { 1, 2, 3, 4 };
							break;
						default: typeList = new int[] { 1, 6, 3, 12 };
							break;
					}
					#endregion

					int rptDtTyp = Convert.ToInt32(table.Rows[0]["RPT_TYP"]);
					List<ReportDate> rptDtList = FUND_RPT_DT_LIST(new ReportDate { Date = newRptDt, Type = rptDtTyp }, typeList, n);
					foreach (ReportDate rpt_dt in rptDtList) {
						returnValue += returnValue != string.Empty ? "," + string.Format("date'{0}'", rpt_dt.Date.ToString("yyyy-MM-dd"))
							: string.Format("date'{0}'", rpt_dt.Date.ToString("yyyy-MM-dd"));
					}

				}
				else//股票、债券
                {
					return ComRptDateSNByRptDate(typ.ToString(), newRptDt.ToString("yyyy-MM-dd"), n.ToString());
				}
			}

			return returnValue;
		}

		/// <summary>
		/// 报告期序列(根据COM_ID返回最新报告期前推或后推N个报告期的序列)
		/// </summary>
		/// <param name="vals">
		/// typ--取报告期的类型（1：最新，2：年报，3：中报或年报，4：季报）
		/// comId--包括股票、基金、债券
		/// n--前推(小于0)后推(大于0)N
		/// </param>
		/// <returns></returns>
		private string RptDateSNByComId(params string[] vals)
		{
			int typ = vals.Length > 0 && vals[0] != string.Empty ? int.Parse(vals[0]) : 0;
			int comId = vals.Length > 1 && vals[1] != string.Empty ? int.Parse(vals[1]) : 0;
			int n = vals.Length > 2 && vals[2] != string.Empty ? int.Parse(vals[2]) : 0;

			string returnValue = string.Empty;
			DataSourceAccess dsAccess = new DataSourceAccess("666");
			dsAccess.Filter = UQuery.Eq("COM_ID", comId);
			DataTable table = dsAccess.Execute();

			if (table.Rows.Count > 0 && n != 0) {
				DateTime newRptDt = Convert.ToDateTime(table.Rows[0]["END_DT"]);
				int typCode = Convert.ToInt32(table.Rows[0]["TYP_CODEI"]);
				if (typCode == 11)//基金
                {
					#region 报告期取值类型列表
					int[] typeList;
					switch (typ) {
						case 1: typeList = new int[] { 1, 6, 3, 12 };
							break;
						case 2: typeList = new int[] { 12 };
							break;
						case 3: typeList = new int[] { 6, 12 };
							break;
						case 4: typeList = new int[] { 1, 2, 3, 4 };
							break;
						default: typeList = new int[] { 1, 6, 3, 12 };
							break;
					}
					#endregion

					int rptDtTyp = Convert.ToInt32(table.Rows[0]["RPT_TYP"]);
					List<ReportDate> rptDtList = FUND_RPT_DT_LIST(new ReportDate { Date = newRptDt, Type = rptDtTyp }, typeList, n);
					foreach (ReportDate rpt_dt in rptDtList) {
						returnValue += returnValue != string.Empty ? "," + string.Format("date'{0}'", rpt_dt.Date.ToString("yyyy-MM-dd"))
							: string.Format("date'{0}'", rpt_dt.Date.ToString("yyyy-MM-dd"));
					}

				}
				else//股票、债券
                {
					return ComRptDateSNByRptDate(typ.ToString(), newRptDt.ToString("yyyy-MM-dd"), n.ToString());
				}
			}

			return returnValue;
		}

		#endregion

		#region 返回最新的基金报告期类型
		/// <summary>
		/// 根据某个日期获取基金最新报告期类型（全部）
		/// </summary>
		/// <param name="date">日期</param>
		/// <returns>最新的基金报告期类型</returns>
		private string FUNC_FUND_RPT_TYP(params string[] vals)
		{
			int typ = vals.Length > 0 && vals[0] != string.Empty ? int.Parse(vals[0]) : 0;
			string date = vals.Length > 1 && vals[1] != string.Empty ? vals[1] : DateTime.Now.ToString("yyyy-MM-dd");
			int rptDtTyp = 0;
			switch (typ) {
				case 1: rptDtTyp = FUND_RPT_DT_NEWEST(Convert.ToDateTime(date)).Type;
					break;
				case 2: rptDtTyp = FUND_RPT_DT_NEWEST(Convert.ToDateTime(date), new int[] { 6, 12 }).Type;
					break;
			}
			return rptDtTyp.ToString();
		}
		#endregion

		#endregion

		#region 其他

		/// <summary>
		/// 根据某个日期获取公司最新报告期，并返回报告期类型
		/// </summary>
		/// <param name="date">指定日期</param>
		/// <returns>最新的股票报告期</returns>
		/// 区间起始	    区间截止	      最新报告期	    最新年报	      最新中报或年报
		/// 2009-11-1   2010-4-30     2009-9-30     2008-12-31    2009-6-30
		/// 2010-5-1    2010-8-31     2010-3-31     2009-12-31    2009-12-31
		/// 2010-9-1    2010-10-31    2010-6-30     2009-12-31    2010-6-30
		/// 2010-11-1   2011-4-30     2010-9-30     2009-12-31    2010-6-30
		private ReportDate COM_RPT_DT_NEWEST(DateTime date)
		{
			int year = date.Year;
			date = date.Date;
			using (DataTable tb = new DataTable()) {
				#region 创建表
				tb.TableName = "COM_RPT_DT_NEWEST";
				tb.Columns.Add(new DataColumn("StartDate", typeof(DateTime)));
				tb.Columns.Add(new DataColumn("EndDate", typeof(DateTime)));
				tb.Columns.Add(new DataColumn("RptDate", typeof(DateTime)));//最新报告期
				tb.Columns.Add(new DataColumn("RptDtTyp", typeof(int)));

				DataRow dr = tb.NewRow();
				dr["StartDate"] = new DateTime(year - 1, 11, 1);
				dr["EndDate"] = new DateTime(year, 4, 30);
				dr["RptDate"] = new DateTime(year - 1, 9, 30);
				dr["RptDtTyp"] = 9;
				tb.Rows.Add(dr);

				dr = tb.NewRow();
				dr["StartDate"] = new DateTime(year, 5, 1);
				dr["EndDate"] = new DateTime(year, 8, 31);
				dr["RptDate"] = new DateTime(year, 3, 31);
				dr["RptDtTyp"] = 1;
				tb.Rows.Add(dr);

				dr = tb.NewRow();
				dr["StartDate"] = new DateTime(year, 9, 1);
				dr["EndDate"] = new DateTime(year, 10, 31);
				dr["RptDate"] = new DateTime(year, 6, 30);
				dr["RptDtTyp"] = 6;
				tb.Rows.Add(dr);

				dr = tb.NewRow();
				dr["StartDate"] = new DateTime(year, 11, 1);
				dr["EndDate"] = new DateTime(year + 1, 4, 30);
				dr["RptDate"] = new DateTime(year, 9, 30);
				dr["RptDtTyp"] = 9;
				tb.Rows.Add(dr);
				#endregion

				ReportDate rptDate = tb.AsEnumerable().Where(row => date >= Convert.ToDateTime(row["StartDate"])
					&& date <= Convert.ToDateTime(row["EndDate"])).Select(item => new ReportDate {
						Type = Convert.ToInt32(item["RptDtTyp"]),
						Date = Convert.ToDateTime(item["RptDate"])
					}).First();
				return rptDate;
			}
		}

		/// <summary>
		/// 根据某个日期获取公司最新报告期，并返回报告期类型
		/// </summary>
		/// <param name="date">指定日期</param>
		/// <param name="typeList">指定的报告期类型列表（1，2，3，4）</param>
		/// <returns>最新的股票报告期</returns>
		private ReportDate COM_RPT_DT_NEWEST(DateTime date, int[] typeList)
		{
			ReportDate rptDt = COM_RPT_DT_NEWEST(date);
			List<ReportDate> dtList = COM_RPT_DT_LIST(rptDt, typeList, -1);
			return dtList.FirstOrDefault();
		}

		/// <summary>
		/// 根据报告期参数，返回在指定类型列表中的N个报告期序列
		/// </summary>
		/// <param name="rptDt">报告期</param>
		/// <param name="typeList">返回的报告期类型列表</param>
		/// <param name="n">前推或后推n个报告期</param>
		/// <returns>报告期序列</returns>
		private List<ReportDate> COM_RPT_DT_LIST(ReportDate rptDt, int[] typeList, int n)
		{
			List<ReportDate> rptDtList = new List<ReportDate>();

			LinkedList<KeyValuePair<int, string>> rptDateLink = new LinkedList<KeyValuePair<int, string>>();
			rptDateLink.AddLast(new KeyValuePair<int, string>(1, "3-31"));
			rptDateLink.AddLast(new KeyValuePair<int, string>(6, "6-30"));
			rptDateLink.AddLast(new KeyValuePair<int, string>(9, "9-30"));
			rptDateLink.AddLast(new KeyValuePair<int, string>(12, "12-31"));

			int yearOfRptDt = rptDt.Date.Year;
			LinkedListNode<KeyValuePair<int, string>> currentNode = rptDateLink.Find(
				new KeyValuePair<int, string>(rptDt.Type, rptDt.Date.ToString("M-dd")));

			if (typeList.Contains(rptDt.Type) && n < 0) {
				rptDtList.Add(rptDt);
			}

			while (rptDtList.Count < Math.Abs(n)) {
				if (n > 0) {
					if (currentNode.Next != null) {
						currentNode = currentNode.Next;
					}
					else {
						currentNode = rptDateLink.First;
						yearOfRptDt++;
					}
				}
				else {
					if (currentNode.Previous != null) {
						currentNode = currentNode.Previous;
					}
					else {
						currentNode = rptDateLink.Last;
						yearOfRptDt--;
					}
				}

				if (typeList.Contains(currentNode.Value.Key)) {
					rptDtList.Add(new ReportDate {
						Type = currentNode.Value.Key,
						Date = DateTime.Parse(string.Format("{0}-{1}", yearOfRptDt, currentNode.Value.Value))
					});
				}
			}
			return rptDtList;
		}

		/// <summary>
		/// 根据某个日期获取基金最新报告期（全部），并返回报告期类型
		/// </summary>
		/// <param name="date">日期</param>
		/// <returns>最新的基金报告期</returns>
		/// 区间起始	                            区间截止	                    最新一期(六)	    最新年报	    最新中报或年报  最新报告期（四）    季报
		/// 2010-9-30+15个工作日+1个自然日	    2010-12-31+15个工作日	    2010年三季报	    2009年年报	2010年中报      2010年三季报       2010年三季报
		/// 2010-9-1		                        2010-9-30+15个工作日        2010年中报	    2009年年报	2010年中报      2010年中报         2010年二季报
		/// 2010-6-30+15个工作日+1个自然日	    2010-8-31	                2010年二季报	    2009年年报	2009年年报      2010年一季报       2010年二季报
		/// 2010-3-31+15个工作日+1个自然日	    2010-6-30+15个工作日	        2010年一季报	    2009年年报	2009年年报      2010年一季报       2010年一季报
		/// 2010-4-1		                        2010-3-31+15个工作日        2009年年报	    2009年年报	2009年年报      2009年年报         2009年四季报
		/// 2009-12-31+15个工作日+1个自然日	    2010-3-31	                2009年四季报	    2008年年报	2009年中报      2009年三季报       2009年四季报
		/// 2009-9-30+15个工作日+1个自然日	    2009-12-31+15个工作日	    2009年三季报	    2008年年报	2009年中报      2009年三季报       2009年三季报
		private ReportDate FUND_RPT_DT_NEWEST(DateTime date)
		{
			int year = date.Year;
			date = date.Date;
			using (DataTable tb = new DataTable()) {
				#region 创建表
				tb.TableName = "FUND_RPT_DT_NEWEST";
				tb.Columns.Add(new DataColumn("StartDate", typeof(DateTime)));
				tb.Columns.Add(new DataColumn("EndDate", typeof(DateTime)));
				tb.Columns.Add(new DataColumn("RptDate", typeof(DateTime)));//最新一期（六）
				tb.Columns.Add(new DataColumn("RptDtTyp", typeof(int)));

				DataRow dr = tb.NewRow();
				dr["StartDate"] = AddTrdDate(new DateTime(year - 1, 9, 30), 15).AddDays(1);//2009-9-30+15个工作日+1个自然日
				dr["EndDate"] = AddTrdDate(new DateTime(year - 1, 12, 31), 15);//2009-12-31+15个工作日
				dr["RptDate"] = new DateTime(year - 1, 9, 30);//2009年三季报
				dr["RptDtTyp"] = 3;
				tb.Rows.Add(dr);

				dr = tb.NewRow();
				dr["StartDate"] = AddTrdDate(new DateTime(year - 1, 12, 31), 15).AddDays(1);//2009-12-31+15个工作日+1个自然日
				dr["EndDate"] = new DateTime(year, 3, 31);//2010-3-31
				dr["RptDate"] = new DateTime(year - 1, 12, 31);//2009年四季报
				dr["RptDtTyp"] = 4;
				tb.Rows.Add(dr);

				dr = tb.NewRow();
				dr["StartDate"] = new DateTime(year, 4, 1);//2010-4-1
				dr["EndDate"] = AddTrdDate(new DateTime(year, 3, 31), 15);//2010-3-31+15个工作日
				dr["RptDate"] = new DateTime(year - 1, 12, 31);//2009年年报
				dr["RptDtTyp"] = 12;
				tb.Rows.Add(dr);

				dr = tb.NewRow();
				dr["StartDate"] = AddTrdDate(new DateTime(year, 3, 31), 15).AddDays(1);//2010-3-31+15个工作日+1个自然日
				dr["EndDate"] = AddTrdDate(new DateTime(year, 6, 30), 15);//2010-6-30+15个工作日
				dr["RptDate"] = new DateTime(year, 3, 31);//2010年一季报
				dr["RptDtTyp"] = 1;
				tb.Rows.Add(dr);

				dr = tb.NewRow();
				dr["StartDate"] = AddTrdDate(new DateTime(year, 6, 30), 15).AddDays(1);//2010-6-30+15个工作日+1个自然日
				dr["EndDate"] = new DateTime(year, 8, 31);//2010-8-31
				dr["RptDate"] = new DateTime(year, 6, 30);//2010年二季报
				dr["RptDtTyp"] = 2;
				tb.Rows.Add(dr);

				dr = tb.NewRow();
				dr["StartDate"] = new DateTime(year, 9, 1);//2010-9-1
				dr["EndDate"] = AddTrdDate(new DateTime(year, 9, 30), 15);//2010-9-30+15个工作日
				dr["RptDate"] = new DateTime(year, 6, 30);//2010年中报
				dr["RptDtTyp"] = 6;
				tb.Rows.Add(dr);

				dr = tb.NewRow();
				dr["StartDate"] = AddTrdDate(new DateTime(year, 9, 30), 15).AddDays(1);//2010-9-30+15个工作日+1个自然日
				dr["EndDate"] = AddTrdDate(new DateTime(year, 12, 31), 15);//2010-12-31+15个工作日
				dr["RptDate"] = new DateTime(year, 9, 30);//2010年三季报
				dr["RptDtTyp"] = 3;
				tb.Rows.Add(dr);

				#endregion

				ReportDate rptDate = tb.AsEnumerable().Where(row => date >= Convert.ToDateTime(row["StartDate"])
					&& date <= Convert.ToDateTime(row["EndDate"])).Select(item => new ReportDate {
						Type = Convert.ToInt32(item["RptDtTyp"]),
						Date = Convert.ToDateTime(item["RptDate"])
					}).First();
				return rptDate;
			}
		}

		/// <summary>
		/// 根据某个日期和指定的报告期类型列表获取基金最新报告期（全部）
		/// </summary>
		/// <param name="date">日期</param>
		/// <param name="typeList">指定的报告期类型列表（1，2，3，4为季报 6 半年报 12 年报）</param>
		/// <returns>最新的基金报告期</returns>
		private ReportDate FUND_RPT_DT_NEWEST(DateTime date, int[] typeList)
		{
			ReportDate rptDt = FUND_RPT_DT_NEWEST(date);
			List<ReportDate> dtList = FUND_RPT_DT_LIST(rptDt, typeList, -1);
			return dtList.FirstOrDefault();
		}

		/// <summary>
		/// 根据报告期参数，返回在指定类型列表中的N个基金报告期
		/// </summary>
		/// <param name="FundRptDate">基金报告期</param>
		/// <param name="typeList">返回的报告期类型列表</param>
		/// <param name="n">前推或后推n个报告期</param>
		/// <returns>基金报告期列表</returns>
		private List<ReportDate> FUND_RPT_DT_LIST(ReportDate rptDt, int[] typeList, int n)
		{
			List<ReportDate> rptDtList = new List<ReportDate>();

			LinkedList<KeyValuePair<int, string>> rptDateLink = new LinkedList<KeyValuePair<int, string>>();
			rptDateLink.AddLast(new KeyValuePair<int, string>(1, "3-31"));
			rptDateLink.AddLast(new KeyValuePair<int, string>(2, "6-30"));
			rptDateLink.AddLast(new KeyValuePair<int, string>(6, "6-30"));
			rptDateLink.AddLast(new KeyValuePair<int, string>(3, "9-30"));
			rptDateLink.AddLast(new KeyValuePair<int, string>(4, "12-31"));
			rptDateLink.AddLast(new KeyValuePair<int, string>(12, "12-31"));

			int yearOfRptDt = rptDt.Date.Year;
			LinkedListNode<KeyValuePair<int, string>> currentNode = rptDateLink.Find(
				new KeyValuePair<int, string>(rptDt.Type, rptDt.Date.ToString("M-dd")));

			if (typeList.Contains(rptDt.Type) && n < 0) {
				rptDtList.Add(rptDt);
			}

			while (rptDtList.Count < Math.Abs(n)) {
				if (n > 0) {
					if (currentNode.Next != null) {
						currentNode = currentNode.Next;
					}
					else {
						currentNode = rptDateLink.First;
						yearOfRptDt++;
					}
				}
				else {
					if (currentNode.Previous != null) {
						currentNode = currentNode.Previous;
					}
					else {
						currentNode = rptDateLink.Last;
						yearOfRptDt--;
					}
				}

				if (typeList.Contains(currentNode.Value.Key)) {
					rptDtList.Add(new ReportDate {
						Type = currentNode.Value.Key,
						Date = DateTime.Parse(string.Format("{0}-{1}", yearOfRptDt, currentNode.Value.Value))
					});
				}
			}

			return rptDtList;
		}

		/// <summary>
		/// 计算某一日期前推或后推N个交易日
		/// </summary>
		/// <param name="date"></param>
		/// <param name="n"></param>
		/// <returns></returns>
		private DateTime AddTrdDate(DateTime date, int n)
		{
			DataSourceAccess dsAccess = new DataSourceAccess("BD_SYS_BAS_TRDT_INFO_1");
			DataTable trdt_info = dsAccess.Execute();
			DateTime returnDate = date.Date;
			if (n < 0)//交易日计算  返回某天（默认为当天）前推或后推N个交易日
            {
				IEnumerable<DateTime> trdates = (from dt in trdt_info.AsEnumerable()
												 where Convert.ToDateTime(dt["NORM_DAY"]) < date.Date
												 orderby dt["NORM_DAY"] descending
												 select Convert.ToDateTime(dt["NORM_DAY"])).Take(Math.Abs(n));
				returnDate = trdates.LastOrDefault();
			}
			if (n > 0)//交易日计算  返回某天（默认为当天）后推N个交易日 
            {
				IEnumerable<DateTime> trdates = (from dt in trdt_info.AsEnumerable()
												 where Convert.ToDateTime(dt["NORM_DAY"]) > date.Date
												 orderby dt["NORM_DAY"] ascending
												 select Convert.ToDateTime(dt["NORM_DAY"])).Take(Math.Abs(n));
				returnDate = trdates.LastOrDefault();
			}
			return returnDate;
		}

		/// <summary>
		/// 判断某个日期是否是交易日
		/// </summary>
		/// <param name="date"></param>
		/// <returns></returns>
		private bool IsTrdDate(DateTime date)
		{
			DataSourceAccess dsAccess = new DataSourceAccess("BD_SYS_BAS_TRDT_INFO_1");
			DataTable trdt_info = dsAccess.Execute();
			bool isTrdDate = (from dtt in trdt_info.AsEnumerable()
							  where Convert.ToDateTime(dtt["NORM_DAY"]) == date.Date
							  select Convert.ToInt32(dtt["IS_TRD_DAY"])).FirstOrDefault() == 1 ? true : false;
			return isTrdDate;
		}

		#endregion
	}
}
