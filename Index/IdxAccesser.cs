using System.Collections.Generic;

namespace BigData.Server.DataAccessHelper.Index
{
	abstract class IdxAccesser
	{
		public IndexObject IdxObj { get; set; }

		public abstract object Execute(IEnumerable<KeyValuePair<string, object>> idxParams);
	}
}
