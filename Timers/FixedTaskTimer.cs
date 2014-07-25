using System;
using System.Timers;

namespace BigData.Server.DataAccessHelper.Timers
{
	class FixedTaskTimer : ITaskTimer
	{
		private Timer _taskTimer;
		private DateTime _nextTaskDate;
		private DateTime _taskDate;

		public DateTime TaskDate
		{
			get { return _taskDate; }
			set { _nextTaskDate = _taskDate = value; }
		}

		public TaskPeriod TaskPeriod { get; set; }

		public event ElapsedEventHandler Elapsed;

		private FixedTaskTimer()
		{
			_taskTimer = new Timer { Interval = 1000, AutoReset = true, Enabled = false };
			_taskTimer.Elapsed += new ElapsedEventHandler(_taskTimer_Elapsed);
		}

		public FixedTaskTimer(DateTime taskDate, TaskPeriod period)
			: this()
		{
			this.TaskPeriod = period;
			this.TaskDate = taskDate;
		}

		private void RefeshNextTaskDate()
		{
			DateTime nowTime = DateTime.Now;
			while (this.TaskPeriod != TaskPeriod.None && _nextTaskDate <= nowTime) {
				switch (this.TaskPeriod) {
					case TaskPeriod.Year:
						int addYears = nowTime.Year - _nextTaskDate.Year;
						addYears = addYears == 0 ? 1 : addYears;
						_nextTaskDate = _nextTaskDate.AddYears(addYears);
						break;
					case TaskPeriod.Month:
						int addMonths = (nowTime.Year - _nextTaskDate.Year) * 12 - _nextTaskDate.Month + nowTime.Month;
						addMonths = addMonths == 0 ? 1 : addMonths;
						_nextTaskDate = _nextTaskDate.AddMonths(addMonths);
						break;
					case TaskPeriod.Day:
						double addDays = Math.Ceiling((nowTime - _nextTaskDate).TotalDays);
						_nextTaskDate = _nextTaskDate.AddDays(addDays);
						break;
				}
			}
		}

		private void _taskTimer_Elapsed(object sender, ElapsedEventArgs e)
		{
			bool doTask = DateTime.Now >= _nextTaskDate && DateTime.Now < _nextTaskDate.AddMilliseconds(_taskTimer.Interval);
			RefeshNextTaskDate();
			if (doTask && Elapsed != null) {
				Elapsed(this, e);
			}
		}

		public void Start()
		{
			_taskTimer.Enabled = true;
		}

		public void Stop()
		{
			_taskTimer.Enabled = false;
		}
	}
}
