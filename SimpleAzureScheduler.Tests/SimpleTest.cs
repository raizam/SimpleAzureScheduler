using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading;
using System.Diagnostics;

namespace SimpleAzureScheduler.Tests
{
    [TestClass]
    public class SimpleTest
    {
        [TestInitialize]
        public void setup()
        {
            Process.Start(@"C:\Program Files\Microsoft SDKs\Windows Azure\Emulator\csrun", "/devstore").WaitForExit();
        }

        public enum Tasks
        {
            Todo
        }

        Random random = new Random();
       
        [TestMethod]
        public void LetsSeeIfEverythingLooksGood()
        {
            using (Scheduler scheduler = new Scheduler())
            {

                for (int i = 0; i < 100; i++)
                {
                    //this is how to schedule a task
                    TimeSpan ts = TimeSpan.FromSeconds(random.Next(10)); // timespan in the next 10 seconds
                    object taskData = new { Title = string.Format("Do it in {0} seconds", ts.TotalSeconds) }; //task data object can be anything System.Helper.Json can handle
                    scheduler.ScheduleTask(Tasks.Todo, ts, taskData); //thread safe
                }

            }//the scheduler must be always disposed to makes sure everything was saved


            using (Scheduler scheduler = new Scheduler())
            {
                

                IDisposable trigger = Execute.AtInterval(TimeSpan.FromSeconds(1), x => //execute every second
                {
                    //this is how to retrive tasks
                    foreach (ScheduledTask task in scheduler.FetchScheduledItems(Tasks.Todo))
                    {
                        dynamic data = task.GetData(); // there's also a generic version: T GetData<T>()
                        string title = data.Title.ToString();

                        Trace.WriteLine(string.Format("Processing task {0} - Category: {1} ", task.Id, title, DateTime.Now.ToLongTimeString()));

                        scheduler.Close(task); //close the task (otherwise it will be executed again later)
                    }
                });

                Thread.Sleep(10000); //wait to let the timer execute incomming tasks

                trigger.Dispose(); //stops executing

            }





        }
    }
}
