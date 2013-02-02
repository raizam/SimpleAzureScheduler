using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Timers;
using System.Threading;

namespace SimpleAzureScheduler
{
    public class Execute : IDisposable
    {

        public static IDisposable AtInterval(IIntervalController intervalController, Action<IIntervalController> action, bool firstLaunchNow = false)
        {
            Execute trigger = new Execute(action, intervalController);
            trigger.controller.Reset();

            trigger.start(firstLaunchNow);
            return trigger;

        }

        public static IDisposable AtInterval(TimeSpan delay, Action<IIntervalController> action, bool firstLaunchNow = false)
        {
            return AtInterval(new DefaultIntervalController(delay), action, firstLaunchNow);

        }

        void IDisposable.Dispose()
        {
            lock (tickLock)
            {
                disposed = true;
                timer.Stop();
            }
        }

       private void onTick(object sender, ElapsedEventArgs e)
        {
            lock (tickLock)
            {
                if (!disposed)
                {
                    _action(controller);
                    timer.Interval = controller.CurrentInterval.TotalMilliseconds;
                    timer.Start();
                }
            }

        }

       private System.Timers.Timer timer = new System.Timers.Timer();



       private Execute(Action<IIntervalController> action, IIntervalController intervalController)
       {
           this.controller = intervalController;
           this._action = action;
       }

       private Action<IIntervalController> _action;

       private void start(bool firstLaunchNow)
       {

           if (timer.Enabled)
               throw new InvalidOperationException("Timer Already running");

           timer.Elapsed += new ElapsedEventHandler(onTick);
           timer.AutoReset = false;

           if (!firstLaunchNow)
           {
               timer.Interval = controller.CurrentInterval.TotalMilliseconds;
           }

           timer.Start();

       }
       private object tickLock = new object();
       private IIntervalController controller = null;
       private volatile bool disposed = false;

    }
}
