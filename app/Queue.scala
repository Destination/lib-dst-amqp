package dst.amqp

import play.Logger

import akka.actor.Actor
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

import scala.collection.mutable

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.{Channel => RMQChannel}
import com.rabbitmq.client.Consumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException

class Queue(val name: String, val channel: RMQChannel) {
  private var consumers: mutable.Map[String, Queue.ActorConsumerAdapter] = mutable.Map.empty

  def bind(exchange: Exchange, routingKey: String) {
    channel.queueBind(name, exchange.name, routingKey)
  }

  def unbind(exchange: Exchange, routingKey: String) {
    channel.queueUnbind(name, exchange.name, routingKey)
  }

  def subscribe(subscriber: ActorRef, autoAck: Boolean = true) : ActorRef = {
    val consumerAdapter = new Queue.ActorConsumerAdapter(subscriber, this)
    channel.basicConsume(name, autoAck, consumerAdapter)
    consumers += (subscriber.path.toString -> consumerAdapter)
    subscriber
  }

  def unsubscribe(subscriber: ActorRef) {
    val path = subscriber.path.toString
    if (consumers contains path) {
      channel.basicCancel(consumers(path).consumerTag)
      consumers -= path
    }
  }

  def ack(deliveryTag: Long) {
    channel.basicAck(deliveryTag, false)
  }
}

object Queue {
  // How long do we wait for incoming messages to be confirmed (ack,reject,accept) by the receiving actor.
  def messageHandlingTimeout = Timeout(1 seconds)

  // Consumer control messages
  trait ControlMessage
  case class ConsumeOk  (consumerTag: String)                                   extends ControlMessage
  case class CancelOk   (consumerTag: String)                                   extends ControlMessage
  case class Cancel     (consumerTag: String)                                   extends ControlMessage
  case class RecoverOk  (consumerTag: String)                                   extends ControlMessage

  // Possible messages to signal channel shutdown
  trait ShutdownMessage
  case class ShutdownSignal(consumerTag: String, sig: ShutdownSignalException)  extends ShutdownMessage
  case object UnexpectedShutdown                                                extends ShutdownMessage

  // Received message envelope
  case class IncomingMessage(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: String)

  // Confirmation messages to let the MQ know what to do with the message
  trait ConfirmationResponse
  case class Ack(multiple: Boolean = false)                                     extends ConfirmationResponse
  case class Reject(requeue: Boolean = false)                                   extends ConfirmationResponse
  case object Accept                                                            extends ConfirmationResponse


  class ActorConsumerAdapter(consumer: ActorRef, queue: Queue) extends Consumer {
    private var _consumerTag: String = null
    def consumerTag: String = _consumerTag

    override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]) {
      Logger.trace(s"Received message from queue: ${queue.name}")

      // send all msgs to the event handler and wait for a confirmation, blocking
      val result = {
        try {
          // actor can die whilst we are waiting which means the channel will be closed...
          val reply = ask(consumer, IncomingMessage(consumerTag, envelope, properties, new String(body, "UTF-8")))(messageHandlingTimeout)
          Await.result(reply, messageHandlingTimeout.duration)
        }
        catch {
          case e : Throwable =>
            Logger.error("Error occured during message delivery, requeueing message", e)
            Reject(true)
        }
      }
      // since we were waiting for the result we need to check if this actor has been terminated and the channel is closed
      if (!queue.channel.isOpen) {
        Logger.error(s"Unable to confirm message '${envelope.getDeliveryTag()}' - channel is already closed, actor must be dead")
        return
      }
      // evaluate the result returned by the handler
      try {
        result match {
          case msg @ Ack(multiple) =>
            Logger.trace(s"Acknowleging '${msg}' message: ${envelope.getDeliveryTag()}")
            queue.channel.basicAck(envelope.getDeliveryTag(), multiple)
          case msg @ Reject(requeue) =>
            Logger.trace(s"Rejecting '${msg}' message: ${envelope.getDeliveryTag()}")
            queue.channel.basicReject(envelope.getDeliveryTag(), requeue)
          case Accept =>
            Logger.trace(s"Message has been accepted by recipient: ${envelope.getDeliveryTag()}")
          case msg =>
            Logger.trace(s"Actor response ${msg} for message ${envelope.getDeliveryTag()} is unknown, dropping message")
            queue.channel.basicReject(envelope.getDeliveryTag(), false)
        }
      }
      // connection must be gone
      catch {
        case e : Throwable =>
          Logger.error("Unable to confirm message, informing actor")
          consumer ! UnexpectedShutdown
      }
    }

    override def handleConsumeOk(consumerTag: String) {
      _consumerTag = consumerTag
      Logger.trace("Consume ok received, informing actor")
      consumer ! ConsumeOk(consumerTag)
    }

    override def handleCancelOk(consumerTag: String) {
      Logger.trace("Cancel ok received, informing actor")
      consumer ! CancelOk(consumerTag)
    }

    override def handleCancel(consumerTag: String) {
      Logger.trace("Cancel received, informing actor")
      consumer ! Cancel(consumerTag)
    }

    override def handleRecoverOk(consumerTag: String) {
      Logger.trace("Recover ok received, informing actor")
      consumer ! RecoverOk(consumerTag)
    }

    override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException) {
      Logger.trace("Connection shutdown, informing actor")
      consumer ! ShutdownSignal(consumerTag, sig)
    }
  }
}
