package kafka.delay.message.utils

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

object DelayServiceConfig {
  val BootstrapServersConfig = "kafka.delay-service.bootstrap-servers"

  @deprecated val InitialTopicsConfig = "kafka.delay-service.initial-topics"

  //在启动时，会忽略其超时时间距当前时间太久的消息。比如忽略超时时间距今1天的消息，这样可以加快启动的速度，以及忽略已无意义的消息。
  //如果一个消息，其超时时间小于当前时间，但是没有被忽略，它会被立即投递给expired topic
  val ignoreExpireAgoConfig = "kafka.delay-service.failover.ignore-expire-ago"
  val ZookeeperConnectString = "kafka.delay-service.zkRoot"

  // 多长时间检查一次新的delay topic
  val NewTopicCheckIntervalConfig = "kafka.delay-service.new-topic-check-interval"

  //跳过failover过程
  val SkipStateRecoveryConfig = "kafka.delay-service.skip-state-recover"

  //是否对从timer拉取的消息进行较验。如果从timer取出的时候，消息的超时时间跟取出的时间大于check-max-diff-ms，就会记一条日志。
  //这样可以发现从timer出来的时候已经超时的消息，从而排除后续步骤IO时间的干扰。
  //主要用于调试。
  val LogTimeDiffEnableConfig = "kafka.delay-service.timer.check-enable"
  val TickMsConfig = "kafka.delay-service.timer.tickMs"
  val MaxTimeDiffMsToLogConfig = "kafka.delay-service.timer.check-max-diff-ms"
  //从timer取到的message metadata以后，如果对应的消息过了这么长时间还没有成功发送到expired topic，就会重试
  //这个时间，应长于Kafka consumer和producer在出错时抛出异常所花时间加上重试时间之和
  val ExpiredSendTimeoutMsConfig = "kafka.delay-service.timer.sendTimeoutMs"
  val LogSendTimeEnableConfig = "kafka.delay-service.timer.logSendTimeEnable"

  val ConsumerCacheMaxBytesConfig = "kafka.delay-service.consumer.cache.maxBytes"

  //TimerConsumer最多缓存多长时间消息的meta，然后开始生成batch
  val MaxStashTimeMsConfig = "kafka.delay-service.batch.batchTimeMs"
  //每个batch内部两个消息内部两个offset的差值最多为多少
  val MaxBatchDistanceConfig = "kafka.delay-service.batch.maxDistance"
  //每个batch里最大的offset和最小offset的差值最多为多少
  val MaxBatchRangeConfig = "kafka.delay-service.batch.maxRange"
  //每个batch以byte计的最大大小是多少，会根据此值估算每次最多fetch多少条消息
  val MaxBatchSizeConfig = "kafka.delay-service.batch.maxSizeInByte"

  //是否允许timerConsumer忽略stash的相关配置，从而拉取所有可能从timer拉取的消息生成batch list，然后再发送
  val DynamicBatchEnableConfig = "kafka.delay-service.batch.dynamic.enable"
  //当拉取到的消息的超时时间跟当拉取时的时间相差超过这个值，就代表着下游的处理能力不足。此时，开始dynamic batch模式。
  //它可以拉取更多的消息的元数据生成batch，从而提高batch的大小以及batch内部offset的连续程度，从而加快下游consumer消费的效率。
  val DynamicBatchMaxDiffMsConfig = "kafka.delay-service.batch.dynamic.maxDiffMs"

  val CacheCheckInterval = FiniteDuration(1, TimeUnit.MINUTES)
  val MiniCacheSize = 1024 * 1024

  def apply(config: Config): DelayServiceConfig = {
    val batchConfig = BatchConfig(
      maxBatchDistance = config.getInt(MaxBatchDistanceConfig),
      maxBatchRange = config.getInt(MaxBatchRangeConfig),
      maxBatchSize = config.getInt(MaxBatchSizeConfig),
      maxStashMs = config.getInt(MaxStashTimeMsConfig),
      dynamicBatchEnable = config.getBoolean(DynamicBatchEnableConfig),
      dynamicBatchMaxDiffMs = config.getInt(DynamicBatchMaxDiffMsConfig)
    )
    DelayServiceConfig(
      tickMs = config.getInt(TickMsConfig),
      logTimeDiffEnable = config.getBoolean(LogTimeDiffEnableConfig),
      timerCheckMaxDiffMs = config.getInt(MaxTimeDiffMsToLogConfig),
      expireSendTimeoutMs = config.getInt(ExpiredSendTimeoutMsConfig),
      enableLogSendTime = config.getBoolean(LogSendTimeEnableConfig),
      batchConfig = batchConfig,
      consumerCacheMaxBytes = config.getInt(ConsumerCacheMaxBytesConfig),
      zkRoot = config.getString(ZookeeperConnectString),
      bootstrapServers = config.getString(BootstrapServersConfig),
      newTopicCheckInterval = config.getDuration(NewTopicCheckIntervalConfig),
      ignoreExpiredMessageBeforeMs = System.currentTimeMillis() - config.getDuration(ignoreExpireAgoConfig).toMillis,
      skipRestoreState = config.getBoolean(SkipStateRecoveryConfig)
    )
  }
}

//class DelayServiceConfig(config: Config) {
//  val TimerCheckModeEnable: Boolean = config.getBoolean(TimerCheckModeEnableConfig)
//  val TimerCheckMaxDiffMs: Int = config.getInt(TimerCheckMaxDiffMsConfig)
//  val MaxBatchSize: Int = config.getInt(MaxBatchSizeConfig)
//  val MaxBatchDistance: Int = config.getInt(MaxBatchDistanceConfig)
//  val MaxBatchRange: Int = config.getInt(MaxBatchRangeConfig)
//  val batchTimeMs: Int = config.getInt(BatchTimeMsConfig)
//  val DynamicBatchEnable: Boolean = config.getBoolean(DynamicBatchConfig)
//  val DynamicBatchMaxSize: Int = config.getInt(DynamicBatchMaxSizeConfig)
//  val DynamicBatchMaxDiffMs: Int = config.getInt(DynamicBatchMaxDiffMsConfig)
//  val BatchListSendingEnable: Boolean = config.getBoolean(BatchListSendingEnableConfig)
//
//  val ConsumerCacheEnable: Boolean = config.getBoolean(ConsumerCacheEnableConfig)
//  val ConsumerCacheMaxBytes: Int = config.getInt(ConsumerCacheMaxBytesConfig)
//
//  val bootstrapServers: String = config.getString(BootstrapServersConfig)
//  //忽略超时时间在此之前的消息
//  val ignoreExpiredMessageBeforeMs = System.currentTimeMillis() - config.getDuration(ignoreExpireAgoConfig).toMillis
//}

case class DelayServiceConfig(tickMs: Int,
                              logTimeDiffEnable: Boolean,
                              timerCheckMaxDiffMs: Int,
                              expireSendTimeoutMs: Int,
                              enableLogSendTime: Boolean,
                              batchConfig: BatchConfig,
                              consumerCacheMaxBytes: Int,
                              zkRoot: String,
                              bootstrapServers: String,
                              newTopicCheckInterval: Duration,
                              //忽略超时时间在此之前的消息
                              ignoreExpiredMessageBeforeMs: Long,
                              skipRestoreState: Boolean
                             )

case class BatchConfig(maxBatchDistance: Int,
                       maxBatchRange: Int,
                       maxBatchSize: Int,
                       maxStashMs: Int,
                       dynamicBatchEnable: Boolean,
                       dynamicBatchMaxDiffMs: Int
                      )