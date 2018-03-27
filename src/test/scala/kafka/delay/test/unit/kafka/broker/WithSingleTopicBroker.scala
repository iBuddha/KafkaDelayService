package kafka.delay.test.unit.kafka.broker

import org.scalatest.{BeforeAndAfterEach, TestSuite}

trait WithSingleTopicBroker extends BeforeAndAfterEach {
  this: TestSuite =>

  private var broker: SeededBroker = null

  override def beforeEach(): Unit = {
    broker = new SeededBroker(topicName, partitionNum)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    broker.shutdown()
    super.afterEach()
  }

  def bootstrapServers: String = broker.getBrokerConnectionString

  protected def topicName: String
  protected def partitionNum: Int

}
