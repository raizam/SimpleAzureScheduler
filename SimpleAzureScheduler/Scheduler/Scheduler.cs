using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Table;
using System.Dynamic;
using System.Web.Helpers;

namespace SimpleAzureScheduler
{
    public partial class Scheduler : IDisposable
    {

        const string StorageConnectionKey = "StorageConnectionString";

        static Scheduler()
        {
            CreateTableIfNotExists();
        }

        private static void CreateTableIfNotExists()
        {
            try
            {
                string connectionString = CloudConfigurationManager.GetSetting(StorageConnectionKey);

#if DEBUG
                if (string.IsNullOrEmpty(connectionString))
                    connectionString = "UseDevelopmentStorage=true";
#endif
                storageAccount = CloudStorageAccount.Parse(connectionString);
                storageAccount.CreateCloudTableClient().GetTableReference(SHEDULER_TABLE).CreateIfNotExists();

            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(string.Format("Check Azure Configuration Settings has a key '{0}' with a valid storage connection string", StorageConnectionKey), ex);

            }
        }

        public Scheduler(bool performImediateCommits = false)
        {
          
            BatchPool = new TableBatchPool(() => this._getTable());
            loop = Task.Factory.StartNew(() =>
            {
                while (keepGoing)
                {
                    if (updateQueue.Count > 0)
                    {
                        UpdateResults();
                    }
                    else
                    {
                        Thread.Sleep(1000);
                    }

                    BatchPool.Commit(performImediateCommits);
                }

                UpdateResults(); //make sure every task enqueued are saved before disposing
                BatchPool.Commit(true);

            }, TaskCreationOptions.LongRunning);

        }




        private void EnqueueTaskResult(ScheduledTask t, ScheduledTaskResult result)
        {

            updateQueue.Enqueue(new ScheduledTaskUpdate { Task = t, Result = result });
        }

        private void UpdateResults()
        {
            ScheduledTaskUpdate taskUpdate;
            while (updateQueue.TryDequeue(out taskUpdate))
            {
                if (taskUpdate.Result.ResultState == ScheduledTaskResultState.Failed)
                {
                    int retry = taskUpdate.Task.FailedTimes + 1;
                    if (retry >= 3)
                    {
                        taskUpdate.Result.ResultState = ScheduledTaskResultState.Unhandled;
                    }
                }

                if (taskUpdate.Result.ResultState != ScheduledTaskResultState.New && taskUpdate.Result.ResultState != ScheduledTaskResultState.Failed)
                    BatchPool.Add(taskUpdate.Task.temporaryTask.PartitionKey, TableOperation.Delete(taskUpdate.Task.temporaryTask));
                var updated = getUpdatedEntity(taskUpdate);

                if (taskUpdate.Result.ResultState == ScheduledTaskResultState.Failed)
                {
                    updated.ETag = "*";
                    BatchPool.Add(updated.PartitionKey, TableOperation.Merge(updated));
                }
                else if (taskUpdate.Result.ResultState != ScheduledTaskResultState.Complete)
                {
                    BatchPool.Add(updated.PartitionKey, TableOperation.Insert(updated));
                }
            }
        }

        private DynamicTableEntity getUpdatedEntity(ScheduledTaskUpdate taskUpdate)
        {
            DynamicTableEntity ent = new DynamicTableEntity();


            ent.RowKey = string.Format("{0}-{1}", DateTime.Now.Add(taskUpdate.Result.NextRunDelay).ToUniversalTime().ToString("yyyyMMddhhmmssffff"), taskUpdate.Task.Id);


            ent["Data"] = new EntityProperty(taskUpdate.Result.Data ?? taskUpdate.Task.Data);

            Exception ex = taskUpdate.Result.Exception;

            int retyCount = taskUpdate.Result.ResultState == ScheduledTaskResultState.Failed ? taskUpdate.Task.FailedTimes + 1 : taskUpdate.Task.FailedTimes;

            if (retyCount > 0)
                ent["RetryCount"] = new EntityProperty(retyCount);

            if (ex != null)
                ent["LastError"] = new EntityProperty(Json.Encode(new { Message = ex.Message, StackTrace = ex.StackTrace, InnerException = ex.InnerException == null ? "" : ex.InnerException.Message }));

            ent["Id"] = new EntityProperty(taskUpdate.Task.Id);

            ent.PartitionKey = taskUpdate.Task.Channel;

            switch (taskUpdate.Result.ResultState)
            {
                case ScheduledTaskResultState.Redirect:

                    ent.PartitionKey = taskUpdate.Result.NewChannel;

                    break;
                case ScheduledTaskResultState.Unhandled:
                    ent.PartitionKey = ent.PartitionKey + "-unhandled";
                    break;
                case ScheduledTaskResultState.Failed:
                    ent.RowKey = taskUpdate.Task.temporaryTask.RowKey;
                    break;
            }

            return ent;
        }

        #region FetchMethod
        /// <summary>
        /// Method Fetching Scheduled Items
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="retryCount"></param>
        /// <returns></returns>
        public ScheduledTask[] FetchScheduledItems(Enum channel)
        {
            return __fetchScheduledItems(_getTable(), _getEnumString(channel), 0);
        }


        public ScheduledTask[] FetchScheduledItems(string channel)
        {
            return __fetchScheduledItems(_getTable(), channel, 0);
        }

        private ScheduledTask[] __fetchScheduledItems(CloudTable scheduleTable, string channel, int retryCount)
        {

            /* ------------------------------------ 
            * Make Range Query to fetch tasks with scheduled time elapsed.
             * The reason we retrieve 50 items is that each row will be followed by two table operations each, which makes a total of 100 operations (Maximum handled by a BatchTableOperation)
            *-------------------------------------
            */

            const int count = 50;

            TableQuery<DynamicTableEntity> rangeQuery = new TableQuery<DynamicTableEntity>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, channel),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, DateTime.Now.ToUniversalTime().ToString("yyyyMMddhhmmssffff"))))
                   .Take(count);

            var results = scheduleTable.ExecuteQuery(rangeQuery).Take(count).ToArray();

            if (results.Length == 0)
                return new ScheduledTask[0];


            /* ------------------------------------ 
             * For every ScheduleEntry retrieved, they must be deleted right away for concurency reason. 
             * They are also 'postponed' at the specifyed time span in order to be fired again in case the task never completes.
             *-------------------------------------
             */

            TableBatchOperation tb = new TableBatchOperation();

            List<ScheduledTask> _items = new List<ScheduledTask>();

            foreach (var x in results)
            {
                int delimiter = x.RowKey.IndexOf('-');
                string[] rowSpl = new string[] { x.RowKey.Substring(0, delimiter), x.RowKey.Substring(delimiter + 1) };

                string tempPostponed = DateTime.Now.ToUniversalTime().AddSeconds(POSTPONE_DELAY_FOR_UNCOMMITED_SECONDS).ToString("yyyyMMddhhmmssffff") + "-" + rowSpl[1];


                DynamicTableEntity ghost = new DynamicTableEntity(x.PartitionKey, tempPostponed);
                ghost.Properties = x.Properties;

                int tryCount = 0;
                if (ghost.Properties.ContainsKey("RetryCount"))
                {
                    tryCount = ghost.Properties["RetryCount"].Int32Value.Value;
                }

                ghost.Properties["RetryCount"] = new EntityProperty(tryCount + 1);




                //delete an postpone
                tb.Add(TableOperation.Delete(x));
                tb.Add(TableOperation.Insert(ghost));

                _items.Add(new ScheduledTask
                {
                    ScheduledTime = DateTime.ParseExact(rowSpl[0], "yyyyMMddhhmmssffff", System.Globalization.CultureInfo.InvariantCulture),
                    Channel = channel,
                    FailedTimes = tryCount,
                    Data = x["Data"].StringValue,
                    Id = rowSpl[1]
                });

            }


            /* ----------------------------------------------------------------------------------------------------------------------------------------
            * Now that the batch operation containing deletes and 'postpones' is built, we execute it.
             * 
             * This BatchOperation is the 'trick' that handles concurency, as the Azure Table Storage is centralized somewhere as one authority, 
             * if two batches are made at the same time on the same rows, one of them will fail, hence it should'nt be possible to dequeue twice 
            *------------------------------------------------------------------------------------------------------------------------------------------
            */
            TableResult[] tableResults = null;
            try
            {
                tableResults = scheduleTable.ExecuteBatch(tb).Where(x => x.HttpStatusCode == 201).ToArray(); //select only 201 response to get only inserted items results
                for (int i = 0; i < _items.Count; i++)
                {
                    _items[i].temporaryTask = (DynamicTableEntity)tableResults[i].Result;
                }
            }
            catch (Exception ex)
            {

             /* ----------------------------------------------------------------------------------------------------------------------------------------
             * If an exception occurs while executing, it's most likely because another Fetch operation where made at the same time (which should have succeed),
             * so we try to execute the FetchScheduledItems operation again to get the next items.
             * 
             * If the exception keeps occuring after several retry times, it's most likely a problem with the Azure Storage Account.
            *------------------------------------------------------------------------------------------------------------------------------------------
                */
                if (retryCount >= 5)
                    throw ex;

                return __fetchScheduledItems(scheduleTable, channel, ++retryCount);
            }

            return _items.ToArray();

        }
        #endregion



        public void Dispose()
        {
            keepGoing = false;
            loop.Wait();
            BatchPool.Dispose();
        }

        #region Schedule methods
        public Guid ScheduleTask(Enum channel, TimeSpan delay, object Data)
        {
           return ScheduleTask(_getEnumString(channel), delay, Data);
        }

        public Guid ScheduleTask(string channel, TimeSpan delay, object Data)
        {
           var taskId = Guid.NewGuid();

            updateQueue.Enqueue(new ScheduledTaskUpdate
            {
                Task = new ScheduledTask { Data = Json.Encode(Data), Channel = channel, Id = taskId.ToString() },
                Result = new ScheduledTaskResult { ResultState = ScheduledTaskResultState.New, NextRunDelay = delay }
            });

            return taskId;
        }

        public Guid PostTask(Enum channel, object Data)
        {
           return ScheduleTask(_getEnumString(channel), new TimeSpan(0, 0, 0), Data);
        }

        public Guid PostTask(string channel, object Data)
        {
           return ScheduleTask(channel, new TimeSpan(0,0,0), Data);
        } 
        #endregion

        internal static string _getEnumString(Enum enumValue)
        {
            return string.Format("{0}{1}", enumValue.GetType().Name, enumValue.ToString());
        }


        private CloudTable _getTable()
        {
            return storageAccount.CreateCloudTableClient().GetTableReference(SHEDULER_TABLE);
        }


        object locker = new object();
        ConcurrentQueue<ScheduledTaskUpdate> updateQueue = new ConcurrentQueue<ScheduledTaskUpdate>();
        Task loop = null;
        volatile bool keepGoing = true;
        TableBatchPool BatchPool;

        static CloudStorageAccount storageAccount;
        public const string SHEDULER_TABLE = "SimpleScheduler";


        const int POSTPONE_DELAY_FOR_UNCOMMITED_SECONDS = 60 * 60 * 6; //6 hours delayed 

    }
}
