using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleAzureScheduler
{
    internal class TableBatchPool : IDisposable
    {

        public class DelayedBatchOperation
        {

            public DelayedBatchOperation(string partition)
            {
                Partition = partition;
                TableBatchOperation = new TableBatchOperation();
                CreationDate = DateTime.Now;
            }
            public string Partition { get; private set; }
            public TableBatchOperation TableBatchOperation { get; private set; }
            public DateTime CreationDate { get; private set; }

        }

        public TableBatchPool(Func<CloudTable> table)
        {
            Table = table;
        }

        TimeSpan DeadLine = new TimeSpan(0, 0, 20);
        Func<CloudTable> Table { get;  set; }
        volatile bool isExecuting = false;

        Dictionary<string, DelayedBatchOperation> updatePool = new Dictionary<string, DelayedBatchOperation>();

        System.Collections.Concurrent.ConcurrentQueue<DelayedBatchOperation> batchQueue = new System.Collections.Concurrent.ConcurrentQueue<DelayedBatchOperation>();

        public void Add(string partition, TableOperation operation)
        {
            if (!updatePool.ContainsKey(partition))
            {
                updatePool[partition] = new DelayedBatchOperation(partition);
            }
            
            updatePool[partition].TableBatchOperation.Add(operation);

            if (updatePool[partition].TableBatchOperation.Count == 100)
            {
                EnqueueBatch(partition);

            }
        }



        private void EnqueueBatch(string partition)
        {
            var toExecute = updatePool[partition];
            updatePool.Remove(partition);
            batchQueue.Enqueue(toExecute);

            if (!isExecuting)
                ExecuteBatch();
        }

        public void Commit(bool force = false)
        {
            foreach(string key in updatePool.Keys.ToArray())
            {

                if (force || updatePool[key].CreationDate.Add(DeadLine) > DateTime.Now)
                {
                    EnqueueBatch(key);
                }

            }
        }

        object executeLock = new object();

        void ExecuteBatch()
        {
            

            lock(executeLock)
            {
                if(isExecuting) 
                    return;

                isExecuting = true;
            }


            _execute(Table());


        }

        void _execute(CloudTable table, TaskCompletionSource<bool> runTask = null)
        {
            DelayedBatchOperation op;
            if (runTask == null)
            {
                runTask = new TaskCompletionSource<bool>(TaskCreationOptions.AttachedToParent);
                var t = runTask.Task;
            }

            if (batchQueue.TryDequeue(out  op))
            {
               
                table.BeginExecuteBatch(op.TableBatchOperation, sync =>
                {
                    var ope = op;
                    try
                    {
                        var resp = table.EndExecuteBatch(sync);
                    }
                    catch (Exception ex)
                    {
                        runTask.SetException(ex);
                        return;
                    }

                    _execute(table, runTask);

                }, null);

            }
            else
            {
                lock (executeLock)
                {
                    isExecuting = false;
                    runTask.SetResult(true);
                }

            }
            
        }


        public void Dispose()
        {
            Commit(true);
        }
    }
}
