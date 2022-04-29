using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;

namespace MISA.Meinvoice.Kinesis.DataErrorWorker
{
    class Program
    {
        static void Main(string[] args)
        {
            IConfiguration Config = new ConfigurationBuilder()
              .AddJsonFile("appsetting.json", optional: false, reloadOnChange: true).Build();
            DataErrorResyncProvider.dbConfig = Config.GetSection("MysqlDB").Value;
            DataErrorResyncProvider.processInterval = int.Parse(Config.GetSection("WorkProcessInterval").Value);
            CreateHostBuilder(args).Build().Run();
        }

        /// <summary>
        /// Build worker đồng bộ dữ liệu lỗi
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
            .ConfigureServices(services =>
                   services.AddHostedService<SyncDataErrorWorker>());
    }
}
