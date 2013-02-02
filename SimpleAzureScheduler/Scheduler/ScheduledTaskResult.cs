using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web.Helpers;

namespace SimpleAzureScheduler
{

    internal enum ScheduledTaskResultState
    {
        New,
        Complete,
        Redirect,
        Failed,
        Postpone,
        Unhandled
    }

    internal class ScheduledTaskResult
    {


        public string NewChannel { get; internal set; }
        public ScheduledTaskResultState ResultState { get; internal set; }
        public TimeSpan NextRunDelay { get; internal set; }
        public Exception Exception { get; internal set; }
        public string Data { get; internal set; }

        internal ScheduledTaskResult()
        {
        }

      
    }
    public partial class Scheduler
    {
        /// <summary>
        /// Set the task has been processed.
        /// This will remove the task completely
        /// </summary>
        /// <param name="task"></param>
        public void Close(ScheduledTask task)
        {
            CheckTask(task);
               this.EnqueueTaskResult(task,  new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Complete });
        }

        ///// <summary>
        ///// Redirects a Task to another channel
        ///// </summary>
        ///// <param name="task"></param>
        ///// <param name="taskChannel"></param>
        ///// <param name="taskdata"></param>
        ///// <param name="delay"></param>
        //public void ScheduleUpdate(ScheduledTask task, TimeSpan nextUpdate, object taskData)
        //{
        //       this.EnqueueTaskResult(task,  new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Complete, NextRunDelay = nextUpdate, Data = Json.Encode(taskData) });

        //}

        ///// <summary>
        ///// The task was succesfully completed, 
        ///// </summary>
        ///// <param name="task"></param>
        ///// <param name="taskChannel"></param>
        ///// <param name="taskdata"></param>
        ///// <param name="delay"></param>
        //public void ScheduleUpdate(ScheduledTask task, TimeSpan nextUpdate)
        //{
        //       this.EnqueueTaskResult(task,  new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Complete, NextRunDelay = nextUpdate });
        //}

        /// <summary>
        /// Redirects a Task to another channel
        /// </summary>
        /// <param name="task"></param>
        /// <param name="taskChannel"></param>
        /// <param name="taskdata"></param>
        /// <param name="delay"></param>
        public void RedirectToChannel(ScheduledTask task, Enum taskChannel, TimeSpan delay)
        {
            CheckTask(task);
               this.EnqueueTaskResult(task,  new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Redirect, NextRunDelay = delay, NewChannel = Scheduler._getEnumString(taskChannel) });
        }

        /// <summary>
        /// Redirects a Task to another channel
        /// </summary>
        /// <param name="task"></param>
        /// <param name="taskChannel"></param>
        /// <param name="taskdata"></param>
        /// <param name="delay"></param>
        public void RedirectToChannel(ScheduledTask task, Enum taskChannel, object taskdata)
        {
            CheckTask(task);
               this.EnqueueTaskResult(task,  new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Redirect, Data = Json.Encode(taskdata), NewChannel = Scheduler._getEnumString(taskChannel) });
        }

        /// <summary>
        /// Redirects a Task to another channel
        /// </summary>
        /// <param name="task"></param>
        /// <param name="taskChannel"></param>
        /// <param name="taskdata"></param>
        /// <param name="delay"></param>
        public void RedirectToChannel(ScheduledTask task, Enum taskChannel, object taskdata, TimeSpan delay)
        {
            CheckTask(task);
               this.EnqueueTaskResult(task,  new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Redirect, NextRunDelay = delay, Data = Json.Encode(taskdata), NewChannel = Scheduler._getEnumString(taskChannel) });
        }

        /// <summary>
        /// Redirects a Task to another channel
        /// </summary>
        /// <param name="task"></param>
        /// <param name="taskChannel"></param>
        /// <param name="taskdata"></param>
        /// <param name="delay"></param>
        public void RedirectToChannel(ScheduledTask task, string taskChannel, TimeSpan delay)
        {
            CheckTask(task);
               this.EnqueueTaskResult(task,  new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Redirect, NextRunDelay = delay, NewChannel = taskChannel });
        }

        /// <summary>
        /// Redirects a Task to another channel
        /// </summary>
        /// <param name="task"></param>
        /// <param name="taskChannel"></param>
        /// <param name="taskdata"></param>
        /// <param name="delay"></param>
        public void RedirectToChannel(ScheduledTask task, string taskChannel, object taskdata)
        {
            CheckTask(task);
               this.EnqueueTaskResult(task,  new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Redirect, Data = Json.Encode(taskdata), NewChannel = taskChannel });
        }

        /// <summary>
        /// Redirects a Task to another channel
        /// </summary>
        /// <param name="task"></param>
        /// <param name="taskChannel"></param>
        /// <param name="taskdata"></param>
        /// <param name="delay"></param>
        public void RedirectToChannel(ScheduledTask task, string taskChannel, object taskdata, TimeSpan delay)
        {
            CheckTask(task);
               this.EnqueueTaskResult(task,  new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Redirect, NextRunDelay = delay, Data = Json.Encode(taskdata), NewChannel = taskChannel });
        }

        /// <summary>
        /// Notify that a technical failure occured. The task will be processed again later.
        /// This will result with the same behavior if it wasn't handled at all, except the exception passed will be saved in the task data.
        /// </summary>
        /// <param name="task"></param>
        public void Failed(ScheduledTask task, Exception ex)
        {
            CheckTask(task);
               this.EnqueueTaskResult(task,  new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Failed, Exception = ex });

        }

        /// <summary>
        /// Notify that it is not possible to handle the task.
        /// The task will be transfered to channel {currentChannel}-unhandled
        /// </summary>
        /// <param name="task"></param>
        public void Unhandled(ScheduledTask task)
        {
            CheckTask(task);
               this.EnqueueTaskResult(task,  new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Unhandled });

        }

        /// <summary>
        /// Reschedule a task to be executed later.
        /// </summary>
        /// <param name="task"></param>
        /// <param name="nextLaunch"></param>
        public void Postpone(ScheduledTask task, TimeSpan nextLaunch)
        {
            CheckTask(task);

            this.EnqueueTaskResult(task, new ScheduledTaskResult { ResultState = ScheduledTaskResultState.Postpone, NextRunDelay = nextLaunch });
        }

        private static void CheckTask(ScheduledTask task)
        {
            if (task.expired)
                throw new InvalidOperationException("The task has already been processed");
            task.expired = true;
        }

    }
}
