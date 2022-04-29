using Amazon.Runtime.CredentialManagement;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MISA.Meinvoice.Kinesis.Credential
{
    public class CredentialWorker : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            CredentialProvider.RunScript();
            await Task.Delay(1000, stoppingToken);
        }
    }

    public class CredentialProvider
    {
        public static void RunScript()
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

        public static string RunCommandWithBash(string command)
        {
            var psi = new ProcessStartInfo();

            psi.FileName = "/bin/bash";
            psi.Arguments = command;
            psi.RedirectStandardOutput = true;
            psi.UseShellExecute = false;
            psi.CreateNoWindow = false;
#if DEBUG
            psi.FileName = "awscredential.bat";
            //psi.Arguments = "awscredential.bat";
#endif
            using var process = Process.Start(psi);

            process.WaitForExit();

            var output = process.StandardOutput.ReadToEnd();

            return output;
        }

        private static void GetAwsSession(string keyId, string secret, string profileName = "default")
        {
            var sharedFile = new SharedCredentialsFile();
            CredentialProfile profile;
            if (sharedFile.TryGetProfile(profileName, out profile))
            {
                var options = new CredentialProfileOptions
                {
                    AccessKey = keyId,
                    SecretKey = secret
                };
                profile.Options.AccessKey = keyId;
                profile.Options.SecretKey = secret;
                sharedFile.RegisterProfile(profile);
            }
        }

        static void ExecuteCommand(string command = "awscredential.bat")
        {
            Console.Error.WriteLine("worker running");
            //int exitCode;
            //ProcessStartInfo processInfo;
            //Process process;

            //processInfo = new ProcessStartInfo("awscredential.bat");
            //processInfo.CreateNoWindow = true;
            //processInfo.UseShellExecute = false;
            //// *** Redirect the output ***
            //processInfo.RedirectStandardError = true;
            //processInfo.RedirectStandardOutput = true;

            //process = Process.Start(processInfo);
            //process.WaitForExit();

            //// *** Read the streams ***
            //// Warning: This approach can lead to deadlocks, see Edit #2
            //string output = process.StandardOutput.ReadToEnd();
            //string error = process.StandardError.ReadToEnd();

            //exitCode = process.ExitCode;

            //Console.WriteLine("output>>" + (String.IsNullOrEmpty(output) ? "(none)" : output));
            //Console.WriteLine("error>>" + (String.IsNullOrEmpty(error) ? "(none)" : error));
            //Console.WriteLine("ExitCode: " + exitCode.ToString(), "ExecuteCommand");
            //process.Close();
        }
    }
}
