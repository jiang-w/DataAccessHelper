using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Timers;

namespace BigData.Server.DataAccessHelper.Timers
{
	class DailyTaskTimer : ITaskTimer
	{
		private Timer _innerTimer;
		private Timer _taskTimer;
		private DateTime _startDate, _endDate;

		public TimeOfDay StartTime { get; private set; }

		public TimeOfDay EndTime { get; private set; }

		public TimeSpan Interval
		{
			get { return TimeSpan.FromMilliseconds(_taskTimer.Interval); }
			set { _taskTimer.Interval = value.TotalMilliseconds; }
		}

		public event ElapsedEventHandler Elapsed;

		private DailyTaskTimer()
		{
			_innerTimer = new Timer { Interval = 1000, AutoReset = true, Enabled = false };
			_innerTimer.Elapsed += new ElapsedEventHandler(_innerTimer_Elapsed);
			_taskTimer = new Timer { AutoReset = true, Enabled = false };
			_taskTimer.Elapsed += new ElapsedEventHandler(_taskTimer_Elapsed);
		}

		public DailyTaskTimer(TimeOfDay startTime, TimeOfDay endTime, TimeSpan interval)
			: this()
		{
			this.StartTime = startTime;
			this.EndTime = endTime;
			this.Interval = interval;
		}

		private void _innerTimer_Elapsed(object sender, ElapsedEventArgs e)
		{
			DateTime nowTime = DateTime.Now;
			if (nowTime >= _startDate && nowTime < _endDate) {
				if (!_taskTimer.Enabled)
					_taskTimer.Enabled = true;
			}
			else {
				if (_taskTimer.Enabled) {
					_taskTimer.Enabled = false;
				}
				if (nowTime >= _endDate) {
					_startDate = _startDate.AddDays(1);
					_endDate = _endDate.AddDays(1);
				}
			}
		}

		private void _taskTimer_Elapsed(object sender, ElapsedEventArgs e)
		{
			if (Elapsed != null)
				Elapsed(this, e);
		}

		public void Start()
		{
			_startDate = DateTime.Now.Date.AddHours(this.StartTime.Hour).AddMinutes(this.StartTime.Minute).AddSeconds(this.StartTime.Second);
			_endDate = DateTime.Now.Date.AddHours(this.EndTime.Hour).AddMinutes(this.EndTime.Minute).AddSeconds(this.EndTime.Second);
			if (_endDate < _startDate)
				_endDate.AddDays(1);
			_innerTimer.Enabled = true;
		}

		public void Stop()
		{
			_innerTimer.Enabled = false;
			if (_taskTimer.Enabled)
				_taskTimer.Enabled = false;
		}
	}

	struct TimeOfDay : IComparable<TimeOfDay>
	{
		private int _hour;
		private int _minute;
		private int _second;

		public int Hour
		{
			get { return _hour; }
		}

		public int Minute
		{
			get { return _minute; }
		}

		public int Second
		{
			get { return _second; }
		}

		public TimeOfDay(int hour, int minute, int second)
		{
			if (hour >= 0 && hour < 24)
				_hour = hour;
			else
				throw new ArgumentOutOfRangeException("Hour", hour, "Hours must be between 0 and 23");

			if (minute >= 0 && minute < 60)
				_minute = minute;
			else
				throw new ArgumentOutOfRangeException("Minute", minute, "Minutes must be between 0 and 60");

			if (second >= 0 && second < 60)
				_second = second;
			else
				throw new ArgumentOutOfRangeException("Second", second, "Seconds must be between 0 and 60");
		}

		public int CompareTo(TimeOfDay value)
		{
			if (this.Hour != value.Hour) {
				return this.Hour.CompareTo(value.Hour);
			}
			else if (this.Minute != value.Minute) {
				return this.Minute.CompareTo(value.Minute);
			}
			else if (this.Second != value.Second) {
				return this.Second.CompareTo(value.Second);
			}
			else {
				return 0;
			}
		}

		public static bool operator <(TimeOfDay time1, TimeOfDay time2)
		{
			return time1.CompareTo(time2) == -1 ? true : false;
		}

		public static bool operator >(TimeOfDay time1, TimeOfDay time2)
		{
			return time1.CompareTo(time2) == 1 ? true : false;
		}

		public static bool operator <=(TimeOfDay time1, TimeOfDay time2)
		{
			int compareValue = time1.CompareTo(time2);
			if (compareValue == -1 || compareValue == 0)
				return true;
			else
				return false;
		}

		public static bool operator >=(TimeOfDay time1, TimeOfDay time2)
		{
			int compareValue = time1.CompareTo(time2);
			if (compareValue == 1 || compareValue == 0)
				return true;
			else
				return false;
		}

		public static TimeSpan operator -(TimeOfDay time1, TimeOfDay time2)
		{
			DateTime date1 = DateTime.Now.Date.AddHours(time1.Hour).AddMinutes(time1.Minute).AddSeconds(time1.Second);
			DateTime date2 = DateTime.Now.Date.AddHours(time2.Hour).AddMinutes(time2.Minute).AddSeconds(time2.Second);
			if (date1 < date2) {
				return date1.AddDays(1) - date2;
			}
			else {
				return date1 - date2;
			}
		}

		public static bool operator ==(TimeOfDay time1, TimeOfDay time2)
		{
			return time1.CompareTo(time2) == 0 ? true : false;
		}

		public static bool operator !=(TimeOfDay time1, TimeOfDay time2)
		{
			return !(time1 == time2);
		}

		public bool Equals(TimeOfDay value)
		{
			return this == value;
		}

		public override bool Equals(object value)
		{
			if (value is TimeOfDay) {
				return this.Equals((TimeOfDay)value);
			}
			else {
				return false;
			}
		}

		public override int GetHashCode()
		{
			return this.ToString().GetHashCode();
		}

		public override string ToString()
		{
			string value = string.Format("{0}:{1}:{2}"
					, this.Hour.ToString().PadLeft(2, '0')
					, this.Minute.ToString().PadLeft(2, '0')
					, this.Second.ToString().PadLeft(2, '0'));
			return value;
		}

		public static TimeOfDay Parse(string s)
		{
			if (Regex.IsMatch(s, @"^(([0-1]?\d)|(2[0-3])):[0-5]?\d:[0-5]?\d$")) {
				int[] paramArray = s.Split(':').Select(item => int.Parse(item)).ToArray();
				return new TimeOfDay(paramArray[0], paramArray[1], paramArray[2]);
			}
			else {
				throw new FormatException("不支持该字符串所表示的 TimeOfDay");
			}
		}
	}
}
