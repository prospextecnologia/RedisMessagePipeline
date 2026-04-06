using Microsoft.Extensions.Logging;
using RedLockNet;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RedisMessagePipeline.Admin
{
    /// <summary>
    /// Admin functionality to manage operations on the Redis pipeline, such as starting, stopping, and cleaning.
    /// </summary>
    public abstract class RedisPipelineBaseAdmin : IRedisPipelineAdmin
    {
        protected readonly ILogger<RedisPipelineBaseAdmin> logger;
        protected readonly RedisPipelineAdminSettings settings;
        protected readonly IDistributedLockFactory lockFactory;
        protected readonly IDatabase database;

        internal RedisPipelineBaseAdmin(
            ILogger<RedisPipelineBaseAdmin> logger,
            RedisPipelineAdminSettings settings,
            IDistributedLockFactory lockFactory,
            IDatabase database)
        {
            this.logger = logger;
            this.settings = settings;
            this.lockFactory = lockFactory;
            this.database = database;
        }

        /// <summary>
        /// Pushes a new message to the Redis pipeline.
        /// </summary>
        public virtual Task<long> PushQueueAsync(RedisValue redisValue)
        {
            if (this.settings.Type != Factory.EnPipelineType.QUEUE)
            {
                throw new Exception("This queue is configured for scheduling; use AddScheduleAsync.");
            }

            logger.LogDebug("Push a new message '{message}' to '{resource}' redis pipeline", redisValue, settings.Resource);

            return Task.FromResult(-1L);

        }

        /// <summary>
        /// Stops the Redis pipeline.
        /// </summary>
        public Task StopAsync()
        {
            logger.LogDebug("Redis pipeline '{resource}' has been stopped", settings.Resource);

            RedisKey key = RedisPipelineExtensions.StateKey(settings.Resource);
            return database.StringSetAsync(key, RedisPipelineExtensions.STATE_STOPPED);
        }

        /// <summary>
        /// Cleans up resources used by the Redis pipeline.
        /// </summary>
        public abstract Task CleanAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Resumes operations of the Redis pipeline after a stop.
        /// </summary>
        public abstract Task ResumeAsync(int skip, CancellationToken cancellationToken);

        public virtual Task AddSheduleAsync(RedisValue keyValue, DateTime shedule, RedisValue redisValue)
        {
            if (this.settings.Type != Factory.EnPipelineType.QUEUE_SCHEDULE)
            {
                throw new Exception("The queue is not configured for scheduling; use PushQueueAsync.");
            }

            logger.LogDebug("Redis pipeline '{resource}' has been stopped", settings.Resource);

            return Task.CompletedTask;
        }

        protected async Task<long> RemoveByPatternInBatchesAsync(string pattern, int batchSize = 500)
        {
            var endpoints = database.Multiplexer.GetEndPoints();
            long totalRemovidas = 0;

            foreach (var endpoint in endpoints)
            {
                var server = database.Multiplexer.GetServer(endpoint);

                if (!server.IsConnected || server.IsReplica)
                    continue;

                var batch = new List<RedisKey>(batchSize);

                foreach (var key in server.Keys(pattern: pattern, pageSize: batchSize))
                {
                    batch.Add(key);
                    if (batch.Count >= batchSize)
                    {
                        totalRemovidas += await database.KeyDeleteAsync(batch.ToArray());
                        batch.Clear();
                    }
                }
                if (batch.Count > 0)
                {
                    totalRemovidas += await database.KeyDeleteAsync(batch.ToArray());
                }
            }

            return totalRemovidas;
        }
    }
}
