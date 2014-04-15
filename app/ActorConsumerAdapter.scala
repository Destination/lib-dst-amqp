package dst.amqp

import play.Logger

import akka.actor.ActorRef
import akka.actor.TypedActor

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Consumer

import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException

class ActorConsumerAdapter(consumer: ActorRef, queue: Queue) extends Consumer with TypedActor.Receiver {
  import TypedActor.dispatcher
  import Queue._

  private var _consumerTag: String = null
  def consumerTag: String = _consumerTag

  def onReceive (message: Any, sender: ActorRef) = {
    message match {
      case msg @ Ack(deliveryTag, multiple) => {
        Logger.debug(s"Acknowleging  message #${deliveryTag}: '${msg}'")
        queue.channel.basicAck(deliveryTag, multiple)
      }
      case msg @ Reject(deliveryTag, requeue) => {
        Logger.debug(s"Rejecting message #${deliveryTag}: '${msg}', requeuing: ${requeue}")
        queue.channel.basicReject(deliveryTag, requeue)
      }
      case Accept(deliveryTag) => {
        Logger.debug(s"Message #{deliveryTag} has been accepted by recipient.")
      }
    }
  }

  def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]) {
    Logger.trace(s"Received message from queue: ${queue.name}")
    consumer ! IncomingMessage(consumerTag, envelope, properties, new String(body, "UTF-8"))
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
