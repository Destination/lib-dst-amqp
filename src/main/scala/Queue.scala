package dst.amqp

import akka.actor.ActorContext
import akka.actor.TypedActor
import akka.actor.TypedProps
import akka.actor.ActorRef

import scala.util.Try
import scala.collection.mutable

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.{Channel => RMQChannel}
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException

class Queue(val name: String, val channel: RMQChannel) {
  private val consumers: mutable.Map[String, ActorConsumer] = mutable.Map.empty

  def bind(exchange: Exchange, routingKey: String) = Try {
    channel.queueBind(name, exchange.name, routingKey)
  }

  def unbind(exchange: Exchange, routingKey: String) = Try {
    channel.queueUnbind(name, exchange.name, routingKey)
  }

  def subscribe(subscriber: ActorRef, autoAck: Boolean = true)(implicit context: ActorContext) : Try[ActorRef] = Try {
    this.synchronized {
      val path = subscriber.path.toString
      val consumerAdapter = TypedActor(context.system).typedActorOf(TypedProps(classOf[ActorConsumer], new ActorConsumerAdapter(subscriber, this)))
      channel.basicConsume(name, autoAck, consumerAdapter)
      consumers += (path -> consumerAdapter)
      subscriber
    }
  }

  def unsubscribe(subscriber: ActorRef)(implicit context: ActorContext) = Try {
    this.synchronized {
      val path = subscriber.path.toString
      if (consumers contains path) {
        val consumerAdapter = consumers(path)
        channel.basicCancel(consumerAdapter.consumerTag)
        TypedActor(context.system).poisonPill(consumerAdapter)
        consumers -= path
      }
    }
  }

  def ack(deliveryTag: Long, multiple: Boolean = false) = Try {
    channel.basicAck(deliveryTag, multiple)
  }

  def nack(deliveryTag: Long, multiple: Boolean = false, requeue: Boolean = false) = Try {
    channel.basicNack(deliveryTag, multiple, requeue)
  }
}

object Queue {
  // Consumer control messages
  trait ControlMessage
  case class ConsumeOk  (consumerTag: String)                                                     extends ControlMessage
  case class CancelOk   (consumerTag: String)                                                     extends ControlMessage
  case class Cancel     (consumerTag: String)                                                     extends ControlMessage
  case class RecoverOk  (consumerTag: String)                                                     extends ControlMessage
  case class ShutdownSignal(consumerTag: String, sig: ShutdownSignalException)                    extends ControlMessage

  // Delivery messages
  trait DeliveryMessage
  case class IncomingMessage(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: String)                                 extends DeliveryMessage
  case class ReturnedMessage(replyCode: Int, replyText: String, exchange: String, routingKey: String, properties: AMQP.BasicProperties, body: String) extends DeliveryMessage

  // Confirmation messages to let the MQ know what to do with the message
  trait ConfirmationResponse
  case class Ack(deliveryTag: Long, multiple: Boolean = false)                                    extends ConfirmationResponse
  case class Nack(deliveryTag: Long, multiple: Boolean = false, requeue: Option[Boolean] = None)  extends ConfirmationResponse
  case class Reject(deliveryTag: Long, requeue: Boolean = false)                                  extends ConfirmationResponse
}
