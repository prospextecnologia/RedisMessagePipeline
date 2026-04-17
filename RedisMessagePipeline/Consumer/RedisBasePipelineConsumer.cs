using Microsoft.Extensions.Logging;
using RedLockNet;
using StackExchange.Redis;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace RedisMessagePipeline.Consumer
{
    /// <summary>
    /// Consumes messages from a Redis pipeline and processes them according to the specified handler logic.
    /// </summary>
    public abstract class RedisBasePipelineConsumer : IRedisPipelineConsumer
    {
        protected readonly ILogger<RedisPipelineQueueConsumer> logger;
        private readonly IRedisPipelineHandler handler;
        protected readonly IDistributedLockFactory lockFactory;
        protected readonly IDatabase database;
        protected readonly RedisPipelineConsumerSettings settings;

        internal RedisBasePipelineConsumer(
            ILogger<RedisPipelineQueueConsumer> logger,
            IRedisPipelineHandler handler,
            RedisPipelineConsumerSettings settings,
            IDistributedLockFactory lockFactory,
            IDatabase database)
        {
            this.logger = logger;
            this.handler = handler;
            this.lockFactory = lockFactory;
            this.database = database;
            this.settings = settings;
        }

        /// <summary>
        /// Executes the consumer processing, continually polling for and handling new messages.
        /// </summary>
        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            logger.LogDebug("RedisPipelineConsumer '{resource}' has been executed.", settings.Resource);

            while (!cancellationToken.IsCancellationRequested)
            {
                bool success = await PollAsync(cancellationToken);
                if (!success)
                {
                    await Task.Delay(settings.PullInterval, cancellationToken);
                }
            }
        }

        /// <summary>
        /// Polls for new messages, processes them, and handles any resulting state changes.
        /// </summary>
        protected abstract Task<bool> PollAsync(CancellationToken cancellationToken);


        /// <summary>
        /// Attempts to process a single message and handle its result.
        /// </summary>
        protected async Task<bool> HandleMessageAsync(RedisValue message, CancellationToken cancellationToken)
        {
            bool success = false;
            try
            {
                success = await handler.HandleAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Handle message '{message}' from redis pipeline '{resource}' has been failed.", message, settings.Resource);
                await StoreFailureAsync(message, ex);
            }

            return success;
        }

        /// <summary>
        /// Records a failure in processing to the Redis failure log.
        /// </summary>
        private async Task StoreFailureAsync(RedisValue message, Exception ex)
        {
            RedisPipelineFailure failure = new RedisPipelineFailure
            {
                Exception = ex.Message,
                Message = message,
                Timestamp = DateTime.UtcNow.Ticks
            };
            await database.StringSetAsync(RedisPipelineExtensions.FailureKey(settings.Resource), JsonSerializer.Serialize(failure));
        }
    }
}
