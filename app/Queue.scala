package dst.amqp

import play.api.Play.current

import play.api.libs.concurrent.Akka
import akka.actor.TypedActor
import akka.actor.TypedProps
import akka.actor.ActorRef

import scala.collection.mutable

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.{Channel => RMQChannel}
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.Consumer
import com.rabbitmq.client.ShutdownSignalException

class Queue(val name: String, val channel: RMQChannel) {
  private val consumers: mutable.Map[String, ActorConsumer] = mutable.Map.empty

  def bind(exchange: Exchange, routingKey: String) = {
    channel.queueBind(name, exchange.name, routingKey)
  }

  def unbind(exchange: Exchange, routingKey: String) = {
    channel.queueUnbind(name, exchange.name, routingKey)
  }

  def subscribe(subscriber: ActorRef, autoAck: Boolean = true) : ActorRef = {
    this.synchronized {
      val path = subscriber.path.toString
      val consumerAdapter = TypedActor(Akka.system).typedActorOf(TypedProps(classOf[ActorConsumer], new ActorConsumerAdapter(subscriber, this)))
      channel.basicConsume(name, autoAck, consumerAdapter)
      consumers += (path -> consumerAdapter)
      subscriber
    }
  }

  def unsubscribe(subscriber: ActorRef) = {
    this.synchronized {
      val path = subscriber.path.toString
      if (consumers contains path) {
        val consumerAdapter = consumers(path)
        channel.basicCancel(consumerAdapter.consumerTag)
        TypedActor(Akka.system).poisonPill(consumerAdapter)
        consumers -= path
      }
    }
  }

  def ack(deliveryTag: Long) = {
    channel.basicAck(deliveryTag, false)
  }
}

object Queue {
  // Consumer control messages
  trait ControlMessage
  case class ConsumeOk  (consumerTag: String)                                                     extends ControlMessage
  case class CancelOk   (consumerTag: String)                                                     extends ControlMessage
  case class Cancel     (consumerTag: String)                                                     extends ControlMessage
  case class RecoverOk  (consumerTag: String)                                                     extends ControlMessage

  // Possible messages to signal channel shutdown
  trait ShutdownMessage
  case class ShutdownSignal(consumerTag: String, sig: ShutdownSignalException)                    extends ShutdownMessage
  case object UnexpectedShutdown                                                                  extends ShutdownMessage

  // Delivery messages
  case class IncomingMessage(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: String)
  case class ReturnedMessage(replyCode: Int, replyText: String, exchange: String, routingKey: String, properties: AMQP.BasicProperties, body: String)

  // Confirmation messages to let the MQ know what to do with the message
  trait ConfirmationResponse
  case class Ack(deliveryTag: Long, multiple: Boolean = false)                                    extends ConfirmationResponse
  case class Nack(deliveryTag: Long, multiple: Boolean = false, requeue: Option[Boolean] = None)  extends ConfirmationResponse
  case class Accept(deliveryTag: Long)                                                            extends ConfirmationResponse
  case class Reject(deliveryTag: Long, requeue: Boolean = false)                                  extends ConfirmationResponse
}
