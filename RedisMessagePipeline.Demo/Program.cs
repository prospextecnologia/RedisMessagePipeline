using Microsoft.Extensions.Logging;
using RedisMessagePipeline;
using RedisMessagePipeline.Admin;
using RedisMessagePipeline.Consumer;
using RedisMessagePipeline.Factory;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;

ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("");
RedLockMultiplexer lockMultiplexer = new RedLockMultiplexer(redis);
IDatabase db = redis.GetDatabase();

var loggerFactory = new LoggerFactory();
RedLockFactory lockFactory = RedLockFactory.Create(new List<RedLockMultiplexer> { lockMultiplexer });
RedisPipelineFactory factory = new RedisPipelineFactory(loggerFactory, lockFactory, db);

var consumer = factory.CreateConsumer(new MyMessageHandler(), new RedisPipelineConsumerSettings("my-messages") { Type = EnPipelineType.QUEUE_SCHEDULE});
var admin = factory.CreateAdmin(new RedisPipelineAdminSettings("my-messages") { Type = EnPipelineType.QUEUE_SCHEDULE });

// ----  Administrate the pipeline  -----

// Stop the pipeline
await admin.StopAsync();

// Push messages
for (int i = 0; i < 10; i++)
{
    await admin.AddScheduleAsync($"{i}", DateTime.Now.AddSeconds(i * 10), $"message:{i}");
}

// Resume the pipeline, skipping problematic messages if necessary
await admin.ResumeAsync(0, CancellationToken.None);


// ------ Start the consumer to process messages ------
await consumer.ExecuteAsync(CancellationToken.None);
/*-----*/

class MyMessageHandler : IRedisPipelineHandler
{
    public async Task<bool> HandleAsync(RedisValue redisValue, CancellationToken cancellationToken)
    {                
        bool success = Random.Shared.Next(0, 100) > 50;
        Console.WriteLine($"{DateTime.Now} Processing task: {redisValue}, success: {success}");
        await Task.Delay(300, cancellationToken);                
        return success;
    }
}