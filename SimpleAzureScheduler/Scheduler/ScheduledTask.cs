using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;
using System.Web.Helpers;

namespace SimpleAzureScheduler
{
    public class ScheduledTask
    {
        internal ScheduledTask() { }

        public DateTime ScheduledTime { get; internal set; }
        public string Channel { get; internal set; }
        public string Id { get; internal set; }
        public string Data { get; internal set; }
        public int FailedTimes { get; internal set; }
        internal DynamicTableEntity temporaryTask { get; set; }

        public dynamic GetData()
        {
            return Json.Decode(Data);
        }

        public T GetData<T>()
        {
            return Json.Decode<T>(Data);
        }

        internal volatile bool expired = false;

    }
}
