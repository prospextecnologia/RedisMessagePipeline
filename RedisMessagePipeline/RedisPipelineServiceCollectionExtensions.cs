using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using RedisMessagePipeline.Factory;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;

namespace RedisMessagePipeline
{
    public static class RedisPipelineServiceCollectionExtensions
    {
        /// <summary>
        /// Adds the <see cref="RedisPipelineFactory"/> service to the specified <see cref="IServiceCollection"/>,
        /// using a provided <see cref="IConnectionMultiplexer"/> from the service provider for Redis connections.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/> to add services to.</param>        
        /// <returns>The original <see cref="IServiceCollection"/> instance, for chaining further calls.</returns>
        public static IServiceCollection AddRedisPipelineFactory(this IServiceCollection services)
        {
            services.TryAddSingleton(sp =>
            {
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                IConnectionMultiplexer multiplexer = sp.GetRequiredService<IConnectionMultiplexer>();
                return CreateRedisPipelineFactory(loggerFactory, multiplexer);
            });
            return services;
        }

        /// <summary>
        /// Adds the <see cref="RedisPipelineFactory"/> service to the specified <see cref="IServiceCollection"/>,
        /// creating a new <see cref="IConnectionMultiplexer"/> instance using the provided Redis connection string.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/> to add services to.</param>
        /// <param name="redisConnectionString">The Redis connection string used to connect to Redis.</param>
        /// <returns>The original <see cref="IServiceCollection"/> instance, for chaining further calls.</returns>
        public static IServiceCollection AddRedisPipelineFactory(this IServiceCollection services, string redisConenctionString)
        {
            services.TryAddSingleton<IConnectionMultiplexer>(sp =>
            {
                var redisOptions = ConfigurationOptions.Parse(redisConenctionString);
                redisOptions.ReconnectRetryPolicy = new ExponentialRetry(1000, 10000);
                return ConnectionMultiplexer.Connect(redisOptions);
            });

            services.AddSingleton<IRedisPipelineFactory>(sp =>
            {
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                var multiplexer = sp.GetRequiredService<IConnectionMultiplexer>();
                return CreateRedisPipelineFactory(loggerFactory, multiplexer);
            });

            return services;
        }


        private static IRedisPipelineFactory CreateRedisPipelineFactory(ILoggerFactory loggerFactory, IConnectionMultiplexer multiplexer)
        {
            IDatabase database = multiplexer.GetDatabase();
            RedLockMultiplexer redLockMultiplexer = new RedLockMultiplexer(multiplexer);
            RedLockFactory lockFactory = RedLockFactory.Create(new[] { redLockMultiplexer });

            return new RedisPipelineFactory(loggerFactory, lockFactory, database);
        }
    }
}
