using System;
using System.Collections.Generic;
using System.Text;

namespace MISA.Meinvoice.Kinesis.Consumer.Library
{
    public class ConsumerConfig
    {
        public static string mysqlDbConfig;
        public static string applicationName;
        public static string secretName;
        public static int maxErrorCountToShutDown = 5;
        public static int backoff;
        public static int checkpointInterval;
        public static int numRetries;
        public static bool logPlaintextData = false;
        public static bool useSecretsManager = true;
        public static int delayRetryTimeMilisecond = 310000;
    }
}
