using Microsoft.Extensions.Logging;
using RedLockNet;
using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace RedisMessagePipeline.Admin
{
    /// <summary>
    /// Admin functionality to manage operations on the Redis pipeline, such as starting, stopping, and cleaning.
    /// </summary>
    public class RedisPipelineQueueAdmin : RedisPipelineBaseAdmin
    {
        internal RedisPipelineQueueAdmin(
            ILogger<RedisPipelineQueueAdmin> logger,
            RedisPipelineAdminSettings settings,
            IDistributedLockFactory lockFactory,
            IDatabase database)
            :base(logger, settings, lockFactory, database)
        {
            
        }

        /// <summary>
        /// Pushes a new message to the Redis pipeline.
        /// </summary>
        public override Task PushQueueAsync(RedisValue redisValue)
        {
            base.PushQueueAsync(redisValue);

            RedisKey key = RedisPipelineExtensions.MessagesListKey(settings.Resource);
            return database.ListRightPushAsync(key, redisValue);
        }
                
        /// <summary>
        /// Cleans up resources used by the Redis pipeline.
        /// </summary>
        public override async Task CleanAsync(CancellationToken cancellationToken)
        {
            using (IRedLock locker = await lockFactory.CreateLockAsync(
                resource: settings.Resource,
                expiryTime: settings.LockSettings.ExpiryTime,
                waitTime: settings.LockSettings.WaitTime,
                retryTime: settings.LockSettings.RetryTime,
                cancellationToken))
            {
                if (!locker.IsAcquired)
                {
                    logger.LogError("Cannot acquire redlock for Redis pipeline '{resource}'", settings.Resource);
                    throw new InvalidOperationException("Cannot acquire redlock");
                }

                await database.KeyDeleteAsync(new RedisKey[]
                {
                    RedisPipelineExtensions.FailureKey(settings.Resource),
                    RedisPipelineExtensions.StateKey(settings.Resource),
                    RedisPipelineExtensions.MessagesListKey(settings.Resource),
                });

                logger.LogDebug("Redis pipeline '{resource}' has been cleaned up", settings.Resource);
            }
        }

        /// <summary>
        /// Resumes operations of the Redis pipeline after a stop.
        /// </summary>
        public override async Task ResumeAsync(int skip, CancellationToken cancellationToken)
        {
            using (IRedLock locker = await lockFactory.CreateLockAsync(
                resource: settings.Resource,
                expiryTime: settings.LockSettings.ExpiryTime,
                waitTime: settings.LockSettings.WaitTime,
                retryTime: settings.LockSettings.RetryTime,
                cancellationToken))
            {
                if (!locker.IsAcquired)
                {
                    logger.LogError("Cannot acquire redlock for Redis pipeline '{resource}'", settings.Resource);
                    throw new InvalidOperationException("Unable to acquire redlock");
                }

                RedisValue state = await database.StringGetAsync(RedisPipelineExtensions.StateKey(settings.Resource));
                if (!RedisPipelineExtensions.IsStopped(state))
                {
                    logger.LogError("Cannot resume '{resource}' redis pipeline that has not stopped", settings.Resource);
                    throw new InvalidOperationException("Cannot resume a pipeline that has not stopped");
                }

                ITransaction transaction = database.CreateTransaction();
                RedisKey messagesKey = RedisPipelineExtensions.MessagesListKey(settings.Resource);
                RedisKey stateKey = RedisPipelineExtensions.StateKey(settings.Resource);
                Task[] transactionTasks = new Task[] {
                    skip > 0 ? transaction.ListLeftPopAsync(messagesKey, count: skip) : Task.CompletedTask,
                    transaction.StringSetAsync(stateKey, 0)
                };
                await transaction.ExecuteAsync();
                await Task.WhenAll(transactionTasks);

                logger.LogDebug("Redis pipeline '{resource}' has been resumed", settings.Resource);
            }
        }
    }
}
