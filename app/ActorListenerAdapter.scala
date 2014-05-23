package dst.amqp

import play.Logger

import akka.actor.ActorRef
import akka.actor.TypedActor

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ReturnListener
import com.rabbitmq.client.ConfirmListener

import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException

trait ActorListener extends ReturnListener with ConfirmListener

class ActorListenerAdapter(listener: ActorRef) extends ActorListener {
  def handleReturn(replyCode: Int, replyText: String, exchange: String, routingKey: String, properties: AMQP.BasicProperties, body: Array[Byte]) {
    val message = new String(body, "UTF-8")
    Logger.trace(s"{consumer.path} - Received returned message: ${replyText}")
    listener ! Queue.ReturnedMessage(replyCode, replyText, exchange, routingKey, properties, message)
  }


  def handleAck(deliveryTag: Long, multiple: Boolean) {
    Logger.trace(s"{consumer.path} - Received message acknowlegement: ${deliveryTag} (multiple=${multiple})")
    listener ! Queue.Ack(deliveryTag, multiple)
  }

  def handleNack(deliveryTag: Long, multiple: Boolean) {
    Logger.trace(s"{consumer.path} - Received lost message: ${deliveryTag} (multiple=${multiple})")
    listener ! Queue.Nack(deliveryTag, multiple)
  }
}

