package dst.amqp

import akka.event.Logging

import akka.actor.ActorContext
import akka.actor.ActorRef
import akka.actor.TypedActor

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ReturnListener
import com.rabbitmq.client.ConfirmListener

import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException

trait ActorListener extends ReturnListener with ConfirmListener

class ActorListenerAdapter(listener: ActorRef)(implicit context: ActorContext) extends ActorListener {
  val Logger = Logging(context.system, context.self)

  def self = TypedActor.get(context.system).getActorRefFor(TypedActor.self[ActorConsumerAdapter])

  def handleReturn(replyCode: Int, replyText: String, exchange: String, routingKey: String, properties: AMQP.BasicProperties, body: Array[Byte]) {
    val message = new String(body, "UTF-8")
    Logger.debug(s"${listener.path} - Received returned message: ${replyText}")
    listener.tell(Queue.ReturnedMessage(replyCode, replyText, exchange, routingKey, properties, message), self)
  }


  def handleAck(deliveryTag: Long, multiple: Boolean) {
    Logger.debug(s"${listener.path} - Received message acknowlegement: ${deliveryTag} (multiple=${multiple})")
    listener.tell(Queue.Ack(deliveryTag, multiple), self)
  }

  def handleNack(deliveryTag: Long, multiple: Boolean) {
    Logger.debug(s"${listener.path} - Received lost message: ${deliveryTag} (multiple=${multiple})")
    listener.tell(Queue.Nack(deliveryTag, multiple), self)
  }
}

