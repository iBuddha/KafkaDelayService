package kafka.delay.test.unit.kafka.broker

/**
  * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
  * See accompanying LICENSE file.
  */

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.google.common.io.Files
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.test.InstanceSpec

import scala.util.Try

/**
  * modified from Kafka Manager of Yahoo
  * @author hiral
  */
class KafkaTestBroker(zookeeper: CuratorFramework, zookeeperConnectionString: String) {
  private[this] val port: Int = InstanceSpec.getRandomPort
  private[this] val config: KafkaConfig = buildKafkaConfig(zookeeperConnectionString)
  private[this] val kafkaServerStartable: KafkaServerStartable = new KafkaServerStartable(config)
  kafkaServerStartable.startup()

  //wait until broker shows up in zookeeper
  var count = 0
  while (count < 10 && zookeeper.checkExists().forPath(kafka.utils.ZkUtils.BrokerIdsPath + "/0") == null) {
    count += 1
    println("Waiting for broker ...")
    println(Option(zookeeper.getData.forPath(kafka.utils.ZkUtils.BrokerIdsPath + "/0")).map(new String(_, StandardCharsets.UTF_8)))
    Thread.sleep(1000)
  }

  private def buildKafkaConfig(zookeeperConnectionString: String): KafkaConfig = {
    val p: Properties = new Properties
    p.setProperty("zookeeper.connect", zookeeperConnectionString)
    p.setProperty("broker.id", "0")
    p.setProperty("port", "" + port)
    p.setProperty("log.dirs", getLogDir)
    p.setProperty("log.retention.hours", "1")
    new KafkaConfig(p)
  }

  private def getLogDir: String = {
    val logDir: File = Files.createTempDir
    logDir.deleteOnExit()
    logDir.getAbsolutePath
  }

  def getBrokerConnectionString: String = s"localhost:$port"

  def getPort: Int = port

  def shutdown() {
    Try(kafkaServerStartable.shutdown())
  }
}
