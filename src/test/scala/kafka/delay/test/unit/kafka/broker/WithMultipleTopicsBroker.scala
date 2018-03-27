package kafka.delay.test.unit.kafka.broker

import org.scalatest.{BeforeAndAfterEach, TestSuite}

trait WithMultipleTopicsBroker extends BeforeAndAfterEach {
  this: TestSuite =>

  private var broker: SeededBroker = null

  override def beforeEach(): Unit = {
    broker = new SeededBroker(topics, true)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    broker.shutdown()
    super.afterEach()
  }

  def bootstrapServers: String = broker.getBrokerConnectionString

  protected def topics: Map[String, Int]

}
