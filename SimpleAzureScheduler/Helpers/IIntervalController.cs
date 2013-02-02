using System;
namespace SimpleAzureScheduler
{
    public interface IIntervalController
    {
        void DecreaseRate();
        void IncreaseRate();
        TimeSpan CurrentInterval { get; }
        void Reset();
    }
}
