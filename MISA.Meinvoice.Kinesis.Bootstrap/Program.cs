using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MISA.Meinvoice.Kinesis.Bootstrap
{
    class Program
    {
        static void Main(string[] args)
        {
            //Init config
            IConfiguration Config = new ConfigurationBuilder()
                .AddJsonFile("bootstrap.json", optional: false, reloadOnChange: true).Build();
            string isWorkerHostedService = Config.GetSection("IsWorkerHostedService").Value;
            string customerConf = Config.GetSection("CustomerConsumerConfig").Value;
            string bankAccountConf = Config.GetSection("BankAccountConsumerConfig").Value;
            string companyConf = Config.GetSection("CompanyConsumerConfig").Value;
            string transActionConf = Config.GetSection("TransactionConsumerConfig").Value;
            DataErrorResyncProvider.dbConfig = Config.GetSection("MysqlDB").Value;
            DataErrorResyncProvider.processInterval = int.Parse(Config.GetSection("WorkProcessInterval").Value);
            List<string> consumerConfig = new List<string>();
            //if (!string.IsNullOrEmpty(customerConf))
            //{
            //    consumerConfig.Add(customerConf);
            //}

            //if (!string.IsNullOrEmpty(bankAccountConf))
            //{
            //    consumerConfig.Add(bankAccountConf);
            //}

            //if (!string.IsNullOrEmpty(companyConf))
            //{
            //    consumerConfig.Add(companyConf);
            //}

            if (!string.IsNullOrEmpty(transActionConf))
            {
                consumerConfig.Add(transActionConf);
            }
             
            InitializeConsumer(consumerConfig);

            if (!string.IsNullOrEmpty(isWorkerHostedService))
            {
                CreateHostBuilder(args).Build().Run();
            }
        }

        private static void InitializeConsumer(List<string> consumerConfig)
        {
            Task t = new Task(() => {
                if (consumerConfig.Count > 1)
                {
                    Parallel.ForEach(consumerConfig, new ParallelOptions() { MaxDegreeOfParallelism = 4 }, config =>
                    {
                        Initialize(config);
                    });
                }
                else if (consumerConfig.Count == 1)
                {
                    Initialize(consumerConfig[0]);
                }
            });
            t.Start();
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

        /// <summary>
        /// Initialize MultiLangDaemon
        /// </summary>
        /// <param name="config"></param>
        public static void Initialize(string config)
        {
            string[] confDetails = config.Split(",");
            string propertiesFileName = confDetails[0];
            string jarFolder = confDetails[1];
            string javaClassPath = FetchJars(jarFolder);
            string java = FindJava();

            if (java == null)
            {
                //Todo: Log lỗi 
                Environment.Exit(2);
            }

            List<string> cmd = new List<string>()
                {
                    java,
                    "-cp",
                    javaClassPath,
                    "software.amazon.kinesis.multilang.MultiLangDaemon",
                    "-p",
                    propertiesFileName
                };
            Process proc = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = cmd[0],
                    Arguments = string.Join(" ", cmd.Skip(1)),
                    UseShellExecute = false
                }
            };
            proc.Start();
            proc.WaitForExit();
        }

        private static string FetchJars(string jarFolder)
        {

            if (!Path.IsPathRooted(jarFolder))
            {
                jarFolder = Path.Combine(Directory.GetCurrentDirectory(), jarFolder);
            }


            List<string> files = Directory.GetFiles(jarFolder).Where(f => f.EndsWith(".jar")).ToList();
            files.Add(Directory.GetCurrentDirectory());
            return string.Join(Path.PathSeparator.ToString(), files);
        }

        private static string FindJava()
        {
            string java = "java";

            Process proc = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = java,
                    Arguments = "-version",
                    UseShellExecute = false
                }
            };
            try
            {
                proc.Start();
                proc.WaitForExit();
                return java;
            }
            catch (Exception ex)
            {
            }
            return null;
        }
    }
}
