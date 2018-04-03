# 功能
构建于Kafka之上的延迟队列。用户使用三个相关联的topic实现延迟队列的功能。

以名为apple的topic为例，用户可以把从apple里读取但尚不能处理的消息发给apple-delay这个topic，消息的key为消息的到期时间(为一个时间戳)，等消息到期后，它会出现在apple-expired这个topic里。此外，用户需要建一个名为apple-delay-meta的topic, 并把它的cleanup.policy设为compact, KafkaDelayService会使用此topic储存被延迟消息的metadata。

用户需要建立四个其名称满足上述规律并有相同分区数量的topic，Kafka delay service会发现有这样规律的一组topic并自动为其建立延迟服务。

用户可以基于此服务自己封装Kafka客户端，以隐藏上述实现细节。比如，当consumer订阅apple这个topic时，封装后的客户端同时自动订阅apple-expired这个topic。

# 启动

```
java -Dlogback.configurationFile="file:logback.xml" -Dconfig.file=application.conf -jar DelayService-assembly-0.1-SNAPSHOT.jar
```

# config 

```hocon
# consumer有时候会卡在(无法退出方法)获取元数据操作上，造成poll()方法无法结束，从使得一个消息的处理无法退出。所以
# consumer需要使用专门的dispatcher，以减少出现上述情况时，饿死其它actor的可能性
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
    core-pool-size-factor = 2.0
    # maximum number of threads to cap factor-based number to
    core-pool-size-max = 16
  }
  # Throughput defines the maximum number of messages to be
  # processed per actor before the thread jumps to the next actor.
  # Set to 1 for as fair as possible.
  throughput = 10
}

# 需要优先保证producer能正常工作，以避免由于consumer的问题导致无法发送超时的消息
producer-dispatcher {
  type = Dispatcher
  executor = "fork-join-executor"
  thread-pool-executor {
    core-pool-size-min = 2
    core-pool-size-factor = 2.0
    core-pool-size-max = 4
  }
  throughput = 1
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
        enable = true
        maxBytes = 1000000
      }
    }
  }
}

```
