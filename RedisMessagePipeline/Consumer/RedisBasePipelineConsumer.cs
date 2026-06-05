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
    public abstract class RedisBasePipelineConsumer : IRedisPipelineConsumer, IDisposable
    {
        protected readonly ILogger<RedisPipelineQueueConsumer> logger;
        protected readonly IRedisPipelineHandler handler;
        protected readonly IDistributedLockFactory lockFactory;
        protected readonly IDatabase database;
        protected readonly RedisPipelineConsumerSettings settings;
        protected readonly IConnectionMultiplexer multiplexer;

        internal RedisBasePipelineConsumer(
            ILogger<RedisPipelineQueueConsumer> logger,
            IRedisPipelineHandler handler,
            RedisPipelineConsumerSettings settings,
            IDistributedLockFactory lockFactory,
            IDatabase database,
            IConnectionMultiplexer multiplexer)
        {
            this.logger = logger;
            this.handler = handler;
            this.lockFactory = lockFactory;
            this.database = database;
            this.settings = settings;
            this.multiplexer = multiplexer;
        }

        
        /// <summary>
        /// Executes the consumer processing, continually polling for and handling new messages.
        /// </summary>
        public virtual async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            logger.LogDebug("RedisPipelineConsumer '{resource}' started.", settings.Resource);

            var subscriber = this.database.Multiplexer.GetSubscriber();
            var channel = RedisChannel.Literal(RedisPipelineExtensions.SignalChannelKey(settings.Resource));
            var signal = new SemaphoreSlim(0, 1);

            await subscriber.SubscribeAsync(channel, (redisChannel, redisValue) =>
            {
                if (signal.CurrentCount == 0)
                    signal.Release();
            });

            try
            {
                // Executa uma vez imediatamente — pode haver item reservado de execução anterior
                signal.Release();


                while (!cancellationToken.IsCancellationRequested)
                {

                    // Aguarda sinal ou timeout de 30s (para recheck periódico)
                    await signal.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);

                    bool success = false;
                    Exception error = null;

                    try
                    {
                        success = await PollAsync(cancellationToken);
                    }
                    catch (OperationCanceledException) { break; }
                    catch (RedisTimeoutException ex)
                    {
                        error = ex;
                        logger.LogError(ex, "Error while polling RedisPipelineConsumer '{resource}'.", settings.Resource);
                        await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        error = ex;
                        logger.LogError(ex, "Error while polling RedisPipelineConsumer '{resource}'.", settings.Resource);
                        await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
                    }


                    if (this.handler != null)
                    {
                        await this.handler.StatusAsync(new RedisConsumerStatus
                        {
                            Resource = settings.Resource,
                            IsAlive = true,
                            ProcessedMessage = success,
                            LastExecutionUtc = DateTime.UtcNow,
                            LastError = error
                        }, cancellationToken);
                    }

                    // Ainda tem itens na fila — continua sem esperar sinal do produtor
                    if (success)
                        signal.Release();

                }
            }
            finally
            {
                await subscriber.UnsubscribeAsync(channel);
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

        public void Dispose()
        {
            multiplexer?.Dispose();
        }
    }
}
