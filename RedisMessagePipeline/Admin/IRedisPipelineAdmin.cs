using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace RedisMessagePipeline.Admin
{
    public interface IRedisPipelineAdmin
    {
        Task<long> PushQueueAsync(RedisValue redisValue);
        Task AddScheduleAsync(RedisValue keyValue, DateTimeOffset schedule, RedisValue redisValue);
        Task StopAsync();
        Task CleanAsync(CancellationToken cancellationToken);
        Task ResumeAsync(int skip, CancellationToken cancellationToken);
    }
}
