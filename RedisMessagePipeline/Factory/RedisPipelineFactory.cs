using Microsoft.Extensions.Logging;
using RedisMessagePipeline.Admin;
using RedisMessagePipeline.Consumer;
using RedLockNet;
using StackExchange.Redis;

namespace RedisMessagePipeline.Factory
{
    /// <summary>
    /// Factory for creating Redis pipeline consumers and admins with configured dependencies.
    /// </summary>
    public class RedisPipelineFactory : IRedisPipelineFactory
    {
        private readonly ILoggerFactory loggerFactory;
        private readonly IDistributedLockFactory lockFactory;
        private readonly IDatabase database;

        public RedisPipelineFactory(ILoggerFactory loggerFactory, IDistributedLockFactory lockFactory, IDatabase database)
        {
            this.loggerFactory = loggerFactory;
            this.lockFactory = lockFactory;
            this.database = database;
        }

        /// <summary>
        /// Creates a RedisPipelineConsumer with specific handler and settings.
        /// </summary>
        public IRedisPipelineConsumer CreateConsumer(IRedisPipelineHandler handler, RedisPipelineConsumerSettings settings)
        {
            switch (settings.Type)
            {
                case EnPipelineType.QUEUE_SCHEDULE:
                    return new RedisPipelineSheduleConsumer(loggerFactory.CreateLogger<RedisPipelineQueueConsumer>(), handler, settings, lockFactory, database);
                default:
                    return new RedisPipelineQueueConsumer(loggerFactory.CreateLogger<RedisPipelineQueueConsumer>(), handler, settings, lockFactory, database);
            }
        }

        /// <summary>
        /// Creates a RedisPipelineAdmin with specific settings.
        /// </summary>
        public IRedisPipelineAdmin CreateAdmin(RedisPipelineAdminSettings settings)
        {
            switch (settings.Type)
            {
                case EnPipelineType.QUEUE_SCHEDULE:
                    return new RedisPipelineSheduleAdmin(loggerFactory.CreateLogger<RedisPipelineSheduleAdmin>(), settings, lockFactory, database);
                default:
                    return new RedisPipelineQueueAdmin(loggerFactory.CreateLogger<RedisPipelineQueueAdmin>(), settings, lockFactory, database);
            }


        }




    }
}
