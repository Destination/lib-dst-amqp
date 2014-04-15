package dst.amqp

import java.util.{Map => JavaMap}
import java.util.{HashMap => JavaHashMap}
import java.lang.{Object => JavaObject}

import com.rabbitmq.client.{Channel => RMQChannel}
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.Consumer
import com.rabbitmq.client.ShutdownSignalException
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ConnectionFactory

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

  def declareExchange(
    name:         String,
    exchangeType: Symbol = 'direct,
    durable:      Boolean = false,
    autoDelete:   Boolean = false,
    arguments:    Map[String, Any] = Map.empty
  ): Exchange = {
    if (! (validExchangeTypes contains exchangeType)) {
      throw new IllegalArgumentException("\"%s\" is not a valid exchange type".format(exchangeType.name))
    }
    channel.exchangeDeclare(name, exchangeType.name, durable, autoDelete, transformArguments(arguments))
    getExchange(name)
  }

  def getExchange(name: String): Exchange = new Exchange(name, channel)

  def deleteExchange(name: String) {
    channel.exchangeDelete(name)
  }

  def declareQueue(
    name:       String,
    durable:    Boolean = false,
    exclusive:  Boolean = false,
    autoDelete: Boolean = false,
    arguments:  Map[String, Any] = Map.empty
  ): Queue = {
    channel.queueDeclare(name, durable, exclusive, autoDelete, transformArguments(arguments))
    getQueue(name)
  }

  def declareQueue(): Queue = {
    val name = channel.queueDeclare().getQueue()
    new Queue(name, channel)
  }

  def getQueue(name: String): Queue = new Queue(name, channel)

  def recover(requeue: Boolean = true) {
    channel.basicRecover(requeue)
  }
}
