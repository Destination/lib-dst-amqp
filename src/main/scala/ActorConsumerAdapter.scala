package dst.amqp

import akka.event.Logging

import akka.actor.ActorContext
import akka.actor.ActorRef
import akka.actor.TypedActor

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Consumer

import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException

trait ActorConsumer extends Consumer {
  def consumerTag: String
}

class ActorConsumerAdapter(consumer: ActorRef, queue: Queue)(implicit context: ActorContext) extends ActorConsumer with TypedActor.Receiver {
  import TypedActor.dispatcher
  import Queue._

  val Logger = Logging(context.system, context.self)

  def self = TypedActor.get(context.system).getActorRefFor(TypedActor.self[ActorConsumerAdapter])

  private var _consumerTag: String = null
  def consumerTag: String = _consumerTag

  def onReceive (message: Any, sender: ActorRef) = {
    message match {
      case msg @ Ack(deliveryTag, multiple) => {
        Logger.debug(s"${consumer.path} - Acknowleging message #${deliveryTag}: '${msg}'")
        queue.channel.basicAck(deliveryTag, multiple)
      }
      case msg @ Nack(deliveryTag, multiple, requeue) => {
        Logger.debug(s"${consumer.path} - Rejecting message #${deliveryTag}: '${msg}'")
        queue.channel.basicNack(deliveryTag, multiple, requeue.getOrElse(false))
      }
      case msg @ Reject(deliveryTag, requeue) => {
        Logger.debug(s"${consumer.path} - Rejecting message #${deliveryTag}: '${msg}', requeuing: ${requeue}")
        queue.channel.basicReject(deliveryTag, requeue)
      }
    }
  }

  def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]) {
    val message = new String(body, "UTF-8")
    Logger.debug(s"${consumer.path} - Received message from queue '${queue.name}': ${message}")
    consumer.tell(IncomingMessage(consumerTag, envelope, properties, message), self)
  }


  override def handleConsumeOk(consumerTag: String) {
    _consumerTag = consumerTag
    Logger.debug(s"${consumer.path} - Consume ok received (${consumerTag})")
    consumer.tell(ConsumeOk(consumerTag), self)
  }

  override def handleCancelOk(consumerTag: String) {
    Logger.debug("${consumer.path} - Cancel ok received, informing actor")
    consumer.tell(CancelOk(consumerTag), self)
  }

  override def handleCancel(consumerTag: String) {
    Logger.debug("${consumer.path} - Cancel received, informing actor")
    consumer.tell(Cancel(consumerTag), self)
  }

  override def handleRecoverOk(consumerTag: String) {
    Logger.debug("${consumer.path} - Recover ok received, informing actor")
    consumer.tell(RecoverOk(consumerTag), self)
  }


  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException) {
    Logger.debug("${consumer.path} - Connection shutdown, informing actor")
    consumer.tell(ShutdownSignal(consumerTag, sig), self)
  }
}
