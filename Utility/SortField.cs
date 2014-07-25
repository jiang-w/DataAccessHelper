using System;
using System.Collections.Generic;

namespace BigData.Server.DataAccessHelper.Utility
{
	public class SortField
	{
		public string FieldName { get; set; }
		public SortMode SortMode { get; set; }

		public SortField(string name, SortMode mode)
		{
			if (string.IsNullOrWhiteSpace(name)) {
				throw new ArgumentNullException("name", "The field name cannot be empty.");
			}
			this.FieldName = name;
			this.SortMode = mode;
		}

		public override string ToString()
		{
			return string.Format("{0} {1}", this.FieldName, this.SortMode);
		}
	}
}
