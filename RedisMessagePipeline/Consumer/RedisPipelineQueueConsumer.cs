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
    public class RedisPipelineQueueConsumer : RedisBasePipelineConsumer
    {
        internal RedisPipelineQueueConsumer(
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

                RedisValue state = await database.StringGetAsync(RedisPipelineExtensions.StateKey(settings.Resource));
                if (RedisPipelineExtensions.IsStopped(state))
                {
                    return false;
                }


                //verifica se este algum item reservado na fila de processamento. 
                long count = await database.ListLengthAsync(RedisPipelineExtensions.MessagesListKey(settings.Reserved));
                if (count > 0)
                {
                    //existe item na fila de reserva, ativa processamento
                    return true;
                }

                //envia objeto para a fila de reserva, para processamento
                RedisValue item = await database.ListMoveAsync(RedisPipelineExtensions.MessagesListKey(settings.Resource), RedisPipelineExtensions.MessagesListKey(settings.Reserved), ListSide.Left, ListSide.Right);
                if (item.IsNull)
                {
                    // fila principal vazia
                    return false;
                }

                return true;
            }
        }


        /// <summary>
        /// Handles successful message processing by resetting the pipeline state and clearing failures.
        /// </summary>
        protected async Task HandleSuccessAsync()
        {
            ITransaction transaction = database.CreateTransaction();
            Task[] transactionTasks = new Task[] {
                transaction.StringSetAsync(RedisPipelineExtensions.StateKey(settings.Resource), 0),
                transaction.KeyDeleteAsync(RedisPipelineExtensions.FailureKey(settings.Resource))
            };
            await transaction.ExecuteAsync();
            await Task.WhenAll(transactionTasks);
        }

        /// <summary>
        /// Handles message processing failures by retrying or stopping the pipeline based on the retry policy.
        /// </summary>
        protected async Task HandleFailureAsync(RedisValue message, RedisValue state)
        {
            logger.LogWarning("Handle message '{message}' from redis pipeline '{resource}' has been failed.", message, settings.Resource);

            await database.ListLeftPushAsync(RedisPipelineExtensions.MessagesListKey(settings.Resource), message);
            RedisKey stateKey = RedisPipelineExtensions.StateKey(settings.Resource);
            if (state.IsNull)
            {
                await database.StringSetAsync(stateKey, 1);
            }
            else if (int.TryParse(state, out int count))
            {
                count++;
                bool shouldStop = count >= settings.MaxRetries;
                await database.StringSetAsync(stateKey, shouldStop ? RedisPipelineExtensions.STATE_STOPPED : $"{count}");

                if (shouldStop)
                {
                    logger.LogWarning("Redis pipeline '{resource}' has been stopped.", settings.Resource);
                }
            }
        }
    }
}
