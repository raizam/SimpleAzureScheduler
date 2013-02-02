using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SimpleAzureScheduler;
using Microsoft.WindowsAzure.Storage.Table;

namespace SimpleAzureScheduler
{
    internal class ScheduledTaskUpdate
    {
        public ScheduledTask Task { get; internal set; }
        public ScheduledTaskResult Result { get; internal set; }


    }
}
