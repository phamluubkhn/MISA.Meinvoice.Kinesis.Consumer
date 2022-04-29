using Microsoft.Extensions.Hosting;
using MISA.Meinvoice.Kinesis.Consumer.Library;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MISA.Meinvoice.Kinesis.Bootstrap
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
        public static string dbConfig = "";
        public static int processInterval = 10;
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
            List<RecordSyncError> recordSyncErrors = MysqlProvider.GetListRecordSyncError(dbConfig);
            Console.Error.WriteLine("Get recordSyncErrors: " + recordSyncErrors.Count);
            if (recordSyncErrors.Count > 0)
            {
                foreach (var item in recordSyncErrors)
                {
                    MysqlProvider.ReSyncData(item.Id, item.RecordData, item.Consumer, dbConfig);
                }
            }
        }
    }
}
