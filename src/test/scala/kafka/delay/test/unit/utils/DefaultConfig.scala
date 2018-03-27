package kafka.delay.test.unit.utils

import com.typesafe.config.ConfigFactory
import kafka.delay.message.utils.DelayServiceConfig

object DefaultConfig {
  private val configStr =
    """
      consumer-dispatcher {
        # Dispatcher is the name of the event-based dispatcher
        # maybe PinnedDispatcher? that is one thread for each consumer
        type = Dispatcher
        # What kind of ExecutionService to use
        executor = "fork-join-executor"
        # Configuration for the fork join pool
        thread-pool-executor {
          # minimum number of threads to cap factor-based core number to
          core-pool-size-min = 3
          # No of core threads ... ceil(available processors * factor)
          core-pool-size-factor = 2.0
          # maximum number of threads to cap factor-based number to
          core-pool-size-max = 16
        }
        # Throughput defines the maximum number of messages to be
        # processed per actor before the thread jumps to the next actor.
        # Set to 1 for as fair as possible.
        throughput = 10
      }

      timer-consumer-dispatcher {
        # Dispatcher is the name of the event-based dispatcher
        # maybe PinnedDispatcher? that is one thread for each consumer
        type = Dispatcher
        # What kind of ExecutionService to use
        executor = "fork-join-executor"
        # Configuration for the fork join pool
        thread-pool-executor {
          # minimum number of threads to cap factor-based core number to
          core-pool-size-min = 3
          # No of core threads ... ceil(available processors * factor)
          core-pool-size-factor = 2.0
          # maximum number of threads to cap factor-based number to
          core-pool-size-max = 16
        }
        # Throughput defines the maximum number of messages to be
        # processed per actor before the thread jumps to the next actor.
        # Set to 1 for as fair as possible.
        throughput = 10
      }

      topic-listener-dispatcher {
        # Dispatcher is the name of the event-based dispatcher
        # maybe PinnedDispatcher? that is one thread for each consumer
        type = Dispatcher
        # What kind of ExecutionService to use
        executor = "fork-join-executor"
        # Configuration for the fork join pool
        thread-pool-executor {
          # minimum number of threads to cap factor-based core number to
          core-pool-size-min = 1
          # No of core threads ... ceil(available processors * factor)
          core-pool-size-factor = 1.0
          # maximum number of threads to cap factor-based number to
          core-pool-size-max = 1
        }
        # Throughput defines the maximum number of messages to be
        # processed per actor before the thread jumps to the next actor.
        # Set to 1 for as fair as possible.
        throughput = 10
      }
      
      # 需要优先保证producer能正常工作，以避免由于consumer的问题导致无法发送超时的消息
      producer-dispatcher {
        # Dispatcher is the name of the event-based dispatcher
        type = Dispatcher
        # What kind of ExecutionService to use
        executor = "fork-join-executor"
        # Configuration for the fork join pool
        thread-pool-executor {
          # minimum number of threads to cap factor-based core number to
          core-pool-size-min = 2
          # No of core threads ... ceil(available processors * factor)
          core-pool-size-factor = 2.0
          # maximum number of threads to cap factor-based number to
          core-pool-size-max = 4
        }
        # Throughput defines the maximum number of messages to be
        # processed per actor before the thread jumps to the next actor.
        # Set to 1 for as fair as possible.
        throughput = 100
      }
      
      
      kafka {
        delay-service {
          #initial-topics = ["BaseTopicA", "BaseTopicB"]
          #https://github.com/typesafehub/config/blob/master/HOCON.md#duration-format
          new-topic-check-interval = "1m"
          bootstrap-servers = "foo:bar"
          zkRoot = ""
          skip-state-recover = false
          failover {
            ignore-expire-ago = "7days"
          }
          timer{
            check-enable = true
            check-max-diff-ms = 3000
            sendTimeoutMs = 180000
            logSendTimeEnable = false
            tickMs = 200
          }
          batch {
            maxSize = 1000
            maxDistance = 500
            batchTimeMs = 500
            maxSizeInByte = 10485760
            maxRange = 5000
            enableBatchList = true
            dynamic {
              enable = true
              maxSize = 10000
              maxDiffMs = 2000
            }
          }
          consumer {
            cache {
              enable = false
              maxBytes = 1000000
            }
          }
        }
      }
      
      akka {
        loggers = ["akka.event.slf4j.Slf4jLogger"]
        loglevel = "DEBUG"
        logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
        actor {
          debug {
            # enable function of LoggingReceive, which is to log any received message at
            #       # DEBUG level
            receive = on
            lifecycle = on
          }
        }
      }
    """.stripMargin

  val config = DelayServiceConfig(ConfigFactory.parseString(configStr))

  def setBootstrapServers(servers: String): DelayServiceConfig = {
    config.copy(bootstrapServers = servers)
  }
}
