using System.Timers;

namespace BigData.Server.DataAccessHelper.Timers
{
	interface ITaskTimer
	{
		event ElapsedEventHandler Elapsed;
		void Start();
		void Stop();
	}
}
