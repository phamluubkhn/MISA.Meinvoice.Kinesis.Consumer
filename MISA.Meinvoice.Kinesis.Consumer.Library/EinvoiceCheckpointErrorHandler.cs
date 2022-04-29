using Amazon.Kinesis.ClientLibrary;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace MISA.Meinvoice.Kinesis.Consumer.Library
{
    /// <summary>
    /// Thử lại checkpoint fail sau một khoảng thời gian, nếu vẫn fail thì log lại vào DB
    /// </summary>
    public static class EinvoiceCheckpointErrorHandler
    {
        public static CheckpointErrorHandler Create(int retries, TimeSpan delay, string kinesisShardId, string configDB, string app)
        {
            return (seq, err, checkpointer) =>
            {
                if (retries > 0)
                {
                    Thread.Sleep(delay);
                    checkpointer.Checkpoint(Create(retries - 1, delay, kinesisShardId, configDB, app));
                }
                else
                {
                    //Log checkpointing fails tại đây
                    MysqlProvider.SaveCheckpointError(KCLApplication.Customer, kinesisShardId, seq, err, configDB);
                }
            };
        }
    }
}
