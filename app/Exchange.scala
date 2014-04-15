package dst.amqp

import com.rabbitmq.client.{
  Channel => RMQChannel,
  MessageProperties
}

class Exchange(val name: String, channel: RMQChannel) {
  private final val defaultEncoding = "UTF-8"

  def publish(routingKey: String, message: String) {
    publish(routingKey, message, defaultEncoding)
  }

  def publish(routingKey: String, message: String, encoding: String) {
    publish(routingKey, message.getBytes(encoding))
  }

  def publish(routingKey: String, message: String, mandatory: Boolean, immediate: Boolean) {
    publish(routingKey, message, defaultEncoding)
  }

  def publish(routingKey: String, message: String, mandatory: Boolean, immediate: Boolean, encoding: String) {
    publish(routingKey, message, encoding)
  }

  def publish(routingKey: String, message: Array[Byte], mandatory: Boolean = false, immediate: Boolean = false) {
    channel.basicPublish(name, routingKey, mandatory, immediate, MessageProperties.TEXT_PLAIN, message)
  }

  def bind(routingKey: String, queues: Queue *) {
    queues.foreach(_.bind(this, routingKey))
  }

  def delete() {
    channel.exchangeDelete(name)
  }
}
