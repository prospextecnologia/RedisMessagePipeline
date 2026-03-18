using StackExchange.Redis;

namespace RedisMessagePipeline
{

    /// <summary>
    /// Helper methods to work with Redis keys and manage the state of Redis pipelines.
    /// </summary>
    public static class RedisPipelineExtensions
    {
        public const string STATE_STOPPED = "STOPPED";

        public static bool IsStopped(RedisValue redisValue) => redisValue.HasValue && redisValue == STATE_STOPPED;
        public static RedisKey MessagesListKey(string resource) => $"{resource}:messages";        
        public static RedisKey StateKey(string resource) => $"{resource}:state";
        public static RedisKey FailureKey(string resource) => $"{resource}:failure";
        public static RedisKey MessagesSortKey(string resource) => $"{resource}:sortkeys";
        public static RedisKey MessageKey(string resource) => $"{resource}:message:";
        public static RedisKey MessageKey(string resource, string id) => $"{MessageKey(resource)}{id}";
    }
}
