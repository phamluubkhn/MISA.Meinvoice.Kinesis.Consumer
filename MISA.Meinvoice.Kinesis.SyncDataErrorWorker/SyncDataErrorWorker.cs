using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MISA.Meinvoice.Kinesis.SyncDataErrorWorker
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
        public static void RunWorker()
        {
            Task t = new Task(() => { DoWork(); });
            t.Start();
        }
        private static void DoWork()
        {
            ExecuteCommand("");
            DateTime now = DateTime.Now;
            DateTime nextTime = now.AddMinutes(10);
            TimeSpan waithTime = nextTime - now;
            Thread.Sleep((int)waithTime.TotalMilliseconds);
            DoWork();
        }      

        static void ExecuteCommand(string command = "awscredential.bat")
        {
            
        }
    }
}
