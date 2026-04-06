using Microsoft.Extensions.Logging;
using RedLockNet;
using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace RedisMessagePipeline.Consumer
{
    /// <summary>
    /// Consumes messages from a Redis pipeline and processes them according to the specified handler logic.
    /// </summary>
    public class RedisPipelineScheduleConsumer : RedisBasePipelineConsumer
    {
        internal RedisPipelineScheduleConsumer(
            ILogger<RedisPipelineQueueConsumer> logger,
            IRedisPipelineHandler handler,
            RedisPipelineConsumerSettings settings,
            IDistributedLockFactory lockFactory,
            IDatabase database)
            : base(logger, handler, settings, lockFactory, database)
        {

        }

        /// <summary>
        /// Polls for new messages, processes them, and handles any resulting state changes.
        /// </summary>
        protected override async Task<bool> PollAsync(CancellationToken cancellationToken)
        {
            try
            {
                //realiza a reserva do item para a fila exclusiva
                await TryDequeueAndReserveAsync(cancellationToken);

                RedisValue message = await database.ListLeftPopAsync(RedisPipelineExtensions.MessagesListKey(settings.Reserved));
                if (message.IsNull)
                {
                    return false;
                }

                bool success = await HandleMessageAsync(message, cancellationToken);
                if (success)
                {
                    await HandleSuccessAsync();
                    return true;
                }
                return false;
            }
            catch (Exception)
            {
                return false;
            }
        }


        private async Task<bool> TryDequeueAndReserveAsync(CancellationToken cancellationToken)
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

                RedisValue state = await database.StringGetAsync(
                    RedisPipelineExtensions.StateKey(settings.Resource));

                if (RedisPipelineExtensions.IsStopped(state))
                {
                    return false;
                }


                //verifica se este algum item reservado na fila de processamento. 
                long count = await database.ListLengthAsync(
                    RedisPipelineExtensions.MessagesListKey(settings.Reserved));
                if (count > 0)
                {
                    //existe item na fila de reserva, ativa processamento
                    return true;
                }

                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                RedisValue[] values = await database.SortedSetRangeByScoreAsync(
                    RedisPipelineExtensions.MessagesSortKey(settings.Resource),
                    stop: now,
                    order: Order.Ascending,
                    take: 1);

                if (values == null || values.Length == 0 || values[0].IsNull)
                {
                    return false;
                }

                string id = values[0].ToString();
                bool removed = await database.SortedSetRemoveAsync(
                    RedisPipelineExtensions.MessagesSortKey(settings.Resource),
                    id);

                if (!removed)
                {
                    return false;
                }

                RedisValue message = await database.StringGetAsync(
                    RedisPipelineExtensions.MessageKey(settings.Resource, id));

                if (message.IsNull)
                {
                    return false;
                }

                //envia para a file de processamento
                long result = await database.ListRightPushAsync(RedisPipelineExtensions.MessagesListKey(settings.Reserved), message);
                if (result <= 0)
                {
                    return false;
                }
                
                await database.KeyDeleteAsync(RedisPipelineExtensions.MessageKey(settings.Resource, id));

                return true;
            }
        }


        /// <summary>
        /// Handles successful message processing by resetting the pipeline state and clearing failures.
        /// </summary>
        private async Task HandleSuccessAsync()
        {
            ITransaction transaction = database.CreateTransaction();
            Task[] transactionTasks = new Task[] {
                transaction.StringSetAsync(RedisPipelineExtensions.StateKey(settings.Resource), 0),
                transaction.KeyDeleteAsync(RedisPipelineExtensions.FailureKey(settings.Resource))
            };
            await transaction.ExecuteAsync();
            await Task.WhenAll(transactionTasks);
        }

        /*
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
        */
    }
}
