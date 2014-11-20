package dst.amqp

import scala.util.Try

import akka.actor.ActorContext

import akka.actor.ActorRef

import java.util.{Map => JavaMap}
import java.util.{HashMap => JavaHashMap}
import java.lang.{Object => JavaObject}

import scala.collection.mutable

import com.rabbitmq.client.{Channel => RMQChannel}

import Utils.transformArguments

private object Utils {
  def transformArguments(inArgs: Map[String, Any]): JavaMap[String, JavaObject] = {
    inArgs.foldLeft(new JavaHashMap[String, JavaObject]) { case (m, (key, value)) =>
      m.put(key, value.asInstanceOf[JavaObject])
      m
    }
  }

  val emptyArguments = transformArguments(Map.empty)
}

class Channel(channel: RMQChannel) {
  private val validExchangeTypes = Set('direct, 'fanout, 'topic)
  private val listeners: mutable.Map[String, ActorListener] = mutable.Map.empty

  def declareExchange(
    name:         String,
    exchangeType: Symbol = 'direct,
    durable:      Boolean = false,
    autoDelete:   Boolean = false,
    arguments:    Map[String, Any] = Map.empty
  ): Try[Exchange] = Try {
    if (! (validExchangeTypes contains exchangeType)) {
      throw new IllegalArgumentException("\"%s\" is not a valid exchange type".format(exchangeType.name))
    }
    channel.exchangeDeclare(name, exchangeType.name, durable, autoDelete, transformArguments(arguments))
    new Exchange(name, channel)
  }

  def deleteExchange(name: String) = Try {
    channel.exchangeDelete(name)
  }

  def declareQueue(
    name:       String,
    durable:    Boolean = false,
    exclusive:  Boolean = false,
    autoDelete: Boolean = false,
    arguments:  Map[String, Any] = Map.empty
  ): Try[Queue] = Try {
    channel.queueDeclare(name, durable, exclusive, autoDelete, transformArguments(arguments))
    new Queue(name, channel)
  }

  def declareQueue(): Try[Queue] = Try {
    val name = channel.queueDeclare().getQueue()
    new Queue(name, channel)
  }

  def recover(requeue: Boolean = true) = Try {
    channel.basicRecover(requeue)
  }


  def addReturnListener(listener: ActorRef)(implicit context: ActorContext): Try[ActorListener] = Try{
    this.synchronized {
      val listenerAdapter = listeners.getOrElseUpdate(listener.path.toString, new ActorListenerAdapter(listener))
      channel.addReturnListener(listenerAdapter)
      listenerAdapter
    }
  }

  def removeReturnListener(listener: ActorRef)(implicit context: ActorContext) = Try {
    val path = listener.path.toString
    this.synchronized {
      if (listeners contains path) {
        val listenerAdapter = listeners(path)
        channel.removeReturnListener(listenerAdapter)
        listeners -= path
      }
    }
  }


  def addConfirmListener(listener: ActorRef)(implicit context: ActorContext): Try[ActorListener] = Try{
    this.synchronized {
      val listenerAdapter = listeners.getOrElseUpdate(listener.path.toString, new ActorListenerAdapter(listener))
      channel.addConfirmListener(listenerAdapter)
      listenerAdapter
    }
  }

  def removeConfirmListener(listener: ActorRef)(implicit context: ActorContext) = Try {
    val path = listener.path.toString
    this.synchronized {
      if (listeners contains path) {
        val listenerAdapter = listeners(path)
        channel.removeConfirmListener(listenerAdapter)
        listeners -= path
      }
    }
  }
}
