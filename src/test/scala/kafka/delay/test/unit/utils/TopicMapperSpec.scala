package kafka.delay.test.unit.utils

import kafka.delay.message.utils.TopicMapper._
import org.scalatest.{FlatSpec, Matchers}
class TopicMapperSpec extends FlatSpec with Matchers {
  "TopicMapper" should "return base name correctly" in {
    val baseName = "test"
    val delayTopic = getDelayTopicName(baseName)
    val expiredTopic = getExpiredTopicName(baseName)
    val metaTopic = getMetaTopicName(baseName)

    delayTopic shouldEqual baseName + delayTopicSuffix
    expiredTopic shouldEqual baseName + expiredTopicSuffix
    metaTopic shouldEqual baseName + metaTopicSuffix

    getBaseName(delayTopic) shouldEqual Some(baseName)
    getBaseName(expiredTopic) shouldEqual Some(baseName)
    getBaseName(metaTopic) shouldEqual Some(baseName)
  }
}
