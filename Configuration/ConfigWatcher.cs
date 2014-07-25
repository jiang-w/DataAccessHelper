using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace BigData.Server.DataAccessHelper.Configuration
{
	class ConfigWatcher
	{
		#region Fields & Properties
		private readonly FileSystemWatcher _watcher;
		private readonly List<string> _files;
		private readonly Timer _delayTimer;

		/// <summary>
		/// 监视文件发生变化后，延迟处理事件的时间（毫秒）
		/// </summary>
		public int DelayMillis { get; set; }

		/// <summary>
		/// 获取或设置要监视的目录的路径
		/// </summary>
		public string Path
		{
			get { return _watcher.Path; }
			set { _watcher.Path = value; }
		}

		/// <summary>
		/// 获取或设置筛选字符串，用于确定在目录中监视哪些文件
		/// </summary>
		public string Filter
		{
			get { return _watcher.Filter; }
			set { _watcher.Filter = value; }
		}

		/// <summary>
		/// 获取或设置要监视的更改类型
		/// </summary>
		public NotifyFilters NotifyFilter
		{
			get { return _watcher.NotifyFilter; }
			set { _watcher.NotifyFilter = value; }
		}

		/// <summary>
		/// 获取或设置一个值，该值指示是否监视指定目录的子目录
		/// </summary>
		public bool IncludeSubdirectories
		{
			get { return _watcher.IncludeSubdirectories; }
			set { _watcher.IncludeSubdirectories = value; }
		}
		#endregion

		#region Constructor
		private ConfigWatcher()
		{
			_watcher = new FileSystemWatcher();
			_watcher.Changed += WaitingFileList_Update;
			_files = new List<string>();
			_delayTimer = new Timer(OnTimer, null, Timeout.Infinite, Timeout.Infinite);
		}

		/// <summary>
		/// 创建一个配置文件监视对象
		/// </summary>
		/// <param name="path">获取或设置要监视的目录的路径</param>
		/// <param name="filter">获取或设置筛选字符串，用于确定在目录中监视哪些文件</param>
		/// <param name="delayMillis">监视文件发生变化后，延迟处理事件的时间（毫秒）</param>
		public ConfigWatcher(string path, string filter, int delayMillis = 1000)
			: this()
		{
			this.Path = path;
			this.Filter = filter;
			this.DelayMillis = delayMillis;
		}
		#endregion

		#region Private Mothd
		private void WaitingFileList_Update(object sender, FileSystemEventArgs e)
		{
			lock (_files) {
				if (!_files.Contains(e.Name)) {
					_files.Add(e.Name);
				}
			}
			_delayTimer.Change(DelayMillis, Timeout.Infinite);
		}

		private void OnTimer(object state)
		{
			List<String> backup = new List<string>();
			backup.AddRange(_files.Distinct());
			_files.Clear();

			foreach (string file in backup) {
				if (Changed != null) {
					Delegate[] delArray = Changed.GetInvocationList();
					foreach (Delegate del in delArray) {
						FileSystemEventHandler method = (FileSystemEventHandler)del;
						method.BeginInvoke(this, new FileSystemEventArgs(WatcherChangeTypes.Changed, this.Path, file)
							, null, null);
					}
				}
			}
		}
		#endregion

		#region Public Mothd
		/// <summary>
		/// 当监视的文件发生变化执行的事件
		/// </summary>
		public event FileSystemEventHandler Changed;

		public void Start()
		{
			_watcher.EnableRaisingEvents = true;
		}

		public void Stop()
		{
			_watcher.EnableRaisingEvents = false;
		}
		#endregion
	}
}
