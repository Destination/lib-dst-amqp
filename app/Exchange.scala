package dst.amqp

import com.rabbitmq.client.{Channel => RMQChannel}
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.AMQP.BasicProperties

class Exchange(val name: String, channel: RMQChannel) {
  private final val defaultEncoding = "UTF-8"
  lazy val properties = new BasicProperties()


  def publish(routingKey: String, message: String, mandatory: Boolean = false, replyTo: Option[String] = None, encoding: String = defaultEncoding) {
    publish(routingKey, message.getBytes(encoding), mandatory, replyTo)
  }

  def publish(routingKey: String, message: Array[Byte], mandatory: Boolean, replyTo: Option[String]) {
    replyTo match {
      case Some(replyTo) => channel.basicPublish(name, routingKey, mandatory, false, properties.builder.deliveryMode(1).replyTo(replyTo).build(), message)
      case None          => channel.basicPublish(name, routingKey, mandatory, false, MessageProperties.TEXT_PLAIN, message)
    }
  }

  def bind(routingKey: String, queues: Queue *) {
    queues.foreach(_.bind(this, routingKey))
  }

  def delete() {
    channel.exchangeDelete(name)
  }
}
