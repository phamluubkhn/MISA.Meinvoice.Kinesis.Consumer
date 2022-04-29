using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace MISA.Meinvoice.Kinesis.ConsumerBootstrap
{
    class Program
    {
        static void Main(string[] args)
        {
            Initialize("transaction.properties,transaction_jars");
        }

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
