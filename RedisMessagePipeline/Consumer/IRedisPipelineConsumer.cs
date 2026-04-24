using System;
using System.Threading;
using System.Threading.Tasks;

namespace RedisMessagePipeline.Consumer
{


    
    public class RedisConsumerStatus
    {
        public string Resource { get; set; }
        public bool IsAlive { get; set; }
        public bool ProcessedMessage { get; set; }
        public DateTime LastExecutionUtc { get; set; }
        public Exception LastError { get; set; }
    }

    public interface IRedisPipelineConsumer
    {
        Task ExecuteAsync(CancellationToken cancellationToken);
    }
}
