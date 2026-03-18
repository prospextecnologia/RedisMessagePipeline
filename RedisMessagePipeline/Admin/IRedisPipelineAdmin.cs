using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace RedisMessagePipeline.Admin
{
    public interface IRedisPipelineAdmin
    {
        Task PushQueueAsync(RedisValue redisValue);
        Task AddSheduleAsync(RedisValue keyValue, DateTime shedule, RedisValue redisValue);
        Task StopAsync();
        Task CleanAsync(CancellationToken cancellationToken);
        Task ResumeAsync(int skip, CancellationToken cancellationToken);
    }
}
