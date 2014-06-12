package dst.amqp

import scala.util.Try

import com.rabbitmq.client.{Channel => RMQChannel}
import com.rabbitmq.client.MessageProperties

class Exchange(val name: String, channel: RMQChannel) {
  import BasicPropertiesImplicit._

  private final val defaultEncoding   = "UTF-8"
  private final val defaultMandatory  = false

  def publish(routingKey: String, message: String, mandatory: Boolean, replyTo: String) {
    publish(routingKey, message, mandatory, BasicProperties(replyTo = Some(replyTo)))
  }
  def publish(routingKey: String, message: String, replyTo: String) {
    publish(routingKey, message, defaultMandatory, BasicProperties(replyTo = Some(replyTo)))
  }


  def publish(routingKey: String, message: String, mandatory: Boolean = defaultMandatory, properties: BasicProperties = BasicProperties.Empty) {
    val encoding = properties.contentEncoding.getOrElse(defaultEncoding)
    publish(routingKey, message.getBytes(encoding), mandatory, properties)
  }

  def publish(routingKey: String, message: Array[Byte], mandatory: Boolean, properties: BasicProperties) = Try {
    channel.basicPublish(name, routingKey, mandatory, false, properties, message)
  }

  def bind(routingKey: String, queues: Queue *) = Try {
    queues.foreach(_.bind(this, routingKey))
  }

  def delete() = Try {
    channel.exchangeDelete(name)
  }
}
