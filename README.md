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
