package dst.amqp

import play.Logger

import akka.actor.ActorRef
import akka.actor.TypedActor

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Consumer

import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException

trait ActorConsumer extends Consumer {
  def consumerTag: String
}

class ActorConsumerAdapter(consumer: ActorRef, queue: Queue) extends ActorConsumer with TypedActor.Receiver {
  import TypedActor.dispatcher
  import Queue._

  private var _consumerTag: String = null
  def consumerTag: String = _consumerTag

  def onReceive (message: Any, sender: ActorRef) = {
    message match {
      case msg @ Ack(deliveryTag, multiple) => {
        Logger.trace(s"{consumer.path} - Acknowleging message #${deliveryTag}: '${msg}'")
        queue.channel.basicAck(deliveryTag, multiple)
      }
      case msg @ Reject(deliveryTag, requeue) => {
        Logger.trace(s"{consumer.path} - Rejecting message #${deliveryTag}: '${msg}', requeuing: ${requeue}")
        queue.channel.basicReject(deliveryTag, requeue)
      }
      case Accept(deliveryTag) => {
        Logger.trace(s"Message #{deliveryTag} has been accepted by {consumer.path}.")
      }
    }
  }

  def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]) {
    val message = new String(body, "UTF-8")
    Logger.trace(s"{consumer.path} - Received message from queue '${queue.name}': ${message}")
    consumer ! IncomingMessage(consumerTag, envelope, properties, message)
  }

  override def handleConsumeOk(consumerTag: String) {
    _consumerTag = consumerTag
    Logger.trace(s"${consumer.path} - Consume ok received (${consumerTag})")
    consumer ! ConsumeOk(consumerTag)
  }

  override def handleCancelOk(consumerTag: String) {
    Logger.trace("{consumer.path} - Cancel ok received, informing actor")
    consumer ! CancelOk(consumerTag)
  }

  override def handleCancel(consumerTag: String) {
    Logger.trace("{consumer.path} - Cancel received, informing actor")
    consumer ! Cancel(consumerTag)
  }

  override def handleRecoverOk(consumerTag: String) {
    Logger.trace("{consumer.path} - Recover ok received, informing actor")
    consumer ! RecoverOk(consumerTag)
  }

  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException) {
    Logger.trace("{consumer.path} - Connection shutdown, informing actor")
    consumer ! ShutdownSignal(consumerTag, sig)
  }
}
