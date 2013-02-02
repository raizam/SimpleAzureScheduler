using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SimpleAzureScheduler
{
    public class DefaultIntervalController : IIntervalController
    {
        double delayOrigin;

        public DefaultIntervalController(TimeSpan delay)
        {
            delayOrigin = currentDelayMilliseconds =  delay.TotalMilliseconds;
        }
        public void IncreaseRate()
        {
            currentDelayMilliseconds += gap();
        }

        public void DecreaseRate()
        {
            currentDelayMilliseconds -= gap();
            if(currentDelayMilliseconds < 10) currentDelayMilliseconds = 10;
        }

        public void Reset()
        {
            currentDelayMilliseconds = delayOrigin;
        }
        private int gap() { return (int)(currentDelayMilliseconds * 0.20); } // 20% of current interval
        internal double currentDelayMilliseconds = 100;
        public TimeSpan CurrentInterval { get { return new TimeSpan(0, 0, 0, 0, (int)currentDelayMilliseconds); } }

    }
}
