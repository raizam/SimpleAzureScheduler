SimpleAzureScheduler
====================

A simple standalone scheduler library relying on Azure Table Storage.

> Currently this is a prototype, a pre-alpha version that still needs to be tested. To be used at your own risk.


##Design goals

* Optimize Storage transactions as much as possible. This is done using RangeQueries and BatchOperations only, grouping transactions as much as possible. Scheduling and Fetching 50 tasks can be done with 3 transactions only.
* Simple and non intrusive API
* Scheduler class has to be thread safe (and should already be)

##How to use it
* Take a look at the code snippet in the unit test project
* The Scheduler class expect to get a storage connection string from the ConfigurationManager, using the key "StorageConnectionString"
* Before compiling don't forget to add "Azure Storage" package from nuget

'''csharp

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

                        Trace.WriteLine(string.Format("{2} - Processed task {0} - Category: {1} ", task.Id, title, DateTime.Now.ToLongTimeString()));

                        scheduler.Close(task); //close the task (otherwise it will be executed again later)
                    }
                });

                Thread.Sleep(10000); //wait to let the timer execute incomming tasks

                trigger.Dispose(); //stops executing

            }

'''