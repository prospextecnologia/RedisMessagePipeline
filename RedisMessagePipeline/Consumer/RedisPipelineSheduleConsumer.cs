using Microsoft.Extensions.Logging;
using RedLockNet;
using StackExchange.Redis;
using System;
using System.Drawing;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace RedisMessagePipeline.Consumer
{
    /// <summary>
    /// Consumes messages from a Redis pipeline and processes them according to the specified handler logic.
    /// </summary>
    public class RedisPipelineSheduleConsumer : RedisBasePipelineConsumer
    {
        internal RedisPipelineSheduleConsumer(
            ILogger<RedisPipelineQueueConsumer> logger,
            IRedisPipelineHandler handler,
            RedisPipelineConsumerSettings settings,
            IDistributedLockFactory lockFactory,
            IDatabase database)
            :base(logger, handler, settings, lockFactory, database)
        {
            
        }

        /// <summary>
        /// Polls for new messages, processes them, and handles any resulting state changes.
        /// </summary>
        protected override async Task<bool> PollAsync(CancellationToken cancellationToken)
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
                    return false;
                }

                RedisValue state = await database.StringGetAsync(RedisPipelineExtensions.StateKey(settings.Resource));
                if (RedisPipelineExtensions.IsStopped(state))
                {
                    return false;
                }

                var stop = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                RedisValue[] values = await database.SortedSetRangeByScoreAsync(
                    RedisPipelineExtensions.MessagesSortKey(settings.Resource),
                    stop: stop,
                    order: Order.Ascending,
                    take: 1
                );

                if (values == null || values.Length <= 0 || values[0].IsNull)
                {
                    return false;
                }

                string value = values[0].ToString(); 

                // Reserva simples: remove da fila
                RedisValue message = await database.SortedSetRemoveAsync(RedisPipelineExtensions.MessagesSortKey(settings.Resource), value);
                if (message.IsNull)
                {
                    return false; //item já foi removido
                }

                message = await database.StringGetAsync(RedisPipelineExtensions.MessageKey(settings.Resource, value));
                if (message.IsNull)
                {
                    return false; 
                }

                bool success = await HandleMessageAsync(message, cancellationToken);
                if (success)
                {
                    await HandleSuccessAsync(value);
                    return true;
                }
                //await HandleFailureAsync(message, state, value);
                return false;
                
            }
        }

        
        /// <summary>
        /// Handles successful message processing by resetting the pipeline state and clearing failures.
        /// </summary>
        private async Task HandleSuccessAsync(RedisValue value)
        {
            ITransaction transaction = database.CreateTransaction();
            Task[] transactionTasks = new Task[] {
                transaction.StringSetAsync(RedisPipelineExtensions.StateKey(settings.Resource), 0),
                transaction.KeyDeleteAsync(RedisPipelineExtensions.FailureKey(settings.Resource)), 
                transaction.KeyDeleteAsync(RedisPipelineExtensions.MessageKey(settings.Resource, value))
            };
            await transaction.ExecuteAsync();
            await Task.WhenAll(transactionTasks);
        }

        /// <summary>
        /// Handles message processing failures by retrying or stopping the pipeline based on the retry policy.
        /// </summary>
        private async Task HandleFailureAsync(RedisValue message, RedisValue state, RedisValue value)
        {
            logger.LogWarning("Handle message '{message}' from redis pipeline '{resource}' has been failed.", message, settings.Resource);
            RedisKey stateKey = RedisPipelineExtensions.StateKey(settings.Resource);
            int count = 1;
            if (!state.IsNull && int.TryParse(state, out count))
            {
                count++;
            }
            count = (count <= 0 ? 1 : count);
            await database.StringSetAsync(stateKey, $"{count}");

            var novaData = DateTime.UtcNow.AddMinutes(count * 60);
            var score = new DateTimeOffset(novaData).ToUnixTimeMilliseconds();
            await database.SortedSetAddAsync(RedisPipelineExtensions.MessagesSortKey(settings.Resource), value, score);
        }
    }
}
