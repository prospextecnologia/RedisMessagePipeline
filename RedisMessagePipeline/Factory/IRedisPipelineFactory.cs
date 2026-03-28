using RedisMessagePipeline.Admin;
using RedisMessagePipeline.Consumer;

namespace RedisMessagePipeline.Factory
{
    public enum EnPipelineType
    {
        QUEUE = 0,
        QUEUE_SCHEDULE = 1
    }

    public interface IRedisPipelineFactory
    {
        IRedisPipelineConsumer CreateConsumer(IRedisPipelineHandler handler, RedisPipelineConsumerSettings settings);
        IRedisPipelineAdmin CreateAdmin(RedisPipelineAdminSettings settings);
    }
}
