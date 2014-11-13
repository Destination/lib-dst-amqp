package dst.amqp

import play.Logger

import play.api.Play.current
import play.api.libs.concurrent.Akka

import akka.actor.ActorRef
import akka.actor.TypedActor

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ReturnListener
import com.rabbitmq.client.ConfirmListener

import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException

trait ActorListener extends ReturnListener with ConfirmListener

class ActorListenerAdapter(listener: ActorRef) extends ActorListener {

  def self = TypedActor.get(Akka.system).getActorRefFor(TypedActor.self[ActorConsumerAdapter])

  def handleReturn(replyCode: Int, replyText: String, exchange: String, routingKey: String, properties: AMQP.BasicProperties, body: Array[Byte]) {
    val message = new String(body, "UTF-8")
    Logger.trace(s"${listener.path} - Received returned message: ${replyText}")
    listener.tell(Queue.ReturnedMessage(replyCode, replyText, exchange, routingKey, properties, message), self)
  }


  def handleAck(deliveryTag: Long, multiple: Boolean) {
    Logger.trace(s"${listener.path} - Received message acknowlegement: ${deliveryTag} (multiple=${multiple})")
    listener.tell(Queue.Ack(deliveryTag, multiple), self)
  }

  def handleNack(deliveryTag: Long, multiple: Boolean) {
    Logger.trace(s"${listener.path} - Received lost message: ${deliveryTag} (multiple=${multiple})")
    listener.tell(Queue.Nack(deliveryTag, multiple), self)
  }
}

