using Microsoft.Extensions.Hosting;
using MISA.Meinvoice.Kinesis.Consumer.Library;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MISA.Meinvoice.Kinesis.DataErrorWorker
{
    public class SyncDataErrorWorker : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            DataErrorResyncProvider.RunWorker();
            await Task.Delay(1000, stoppingToken);
        }
    }

    public class DataErrorResyncProvider
    {
        public static string dbConfig = DataErrorResyncProvider.dbConfig;
        public static int processInterval = DataErrorResyncProvider.processInterval;
        public static void RunWorker()
        {
            Task t = new Task(() => { DoWork(dbConfig); });
            t.Start();
        }
        private static void DoWork(string dbConfig)
        {
            ProcessResyncDataError(dbConfig);
            DateTime now = DateTime.Now;
            DateTime nextTime = now.AddMinutes(processInterval);
            TimeSpan waithTime = nextTime - now;
            Thread.Sleep((int)waithTime.TotalMilliseconds);
            DoWork(dbConfig);
        }

        static void ProcessResyncDataError(string dbConfig)
        {
            List<CommandSyncError> commandSyncErrors = MysqlProvider.GetListCommandSyncError(dbConfig);
            Console.Error.WriteLine("Get recordSyncErrors: " + commandSyncErrors.Count);
            if (commandSyncErrors.Count > 0)
            {
                foreach (var item in commandSyncErrors)
                {
                    MysqlProvider.RetryCommandSyncError(item, dbConfig);
                }
            }
        }
    }
}
