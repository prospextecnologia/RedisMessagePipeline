using System;

namespace RedisMessagePipeline
{
    /// <summary>
    /// Configuration for the distributed locks used by Redis pipelines to ensure safe concurrent operations.
    /// </summary>
    public class RedisPipelineLockSettings
    {
        public TimeSpan ExpiryTime { get; set; } = TimeSpan.FromSeconds(15);
        public TimeSpan WaitTime { get; set; } = TimeSpan.FromMilliseconds(5);
        public TimeSpan RetryTime { get; set; } = TimeSpan.FromMilliseconds(500);
    }
}
