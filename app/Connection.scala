package dst.amqp

import com.rabbitmq.client.ConnectionFactory

class Connection(connectionFactory: ConnectionFactory = new ConnectionFactory()) {
  def this(
    hostName:     Option[String] =  None,
    portNumber:   Option[Int] =     None,
    virtualHost:  Option[String] =  None,
    userName:     Option[String] =  None,
    password:     Option[String] =  None
  ) = this({
    val factory = new ConnectionFactory()
    hostName.map(factory.setHost(_));
    portNumber.map(factory.setPort(_))
    virtualHost.map(factory.setVirtualHost(_))
    userName.map(factory.setUsername(_))
    password.map(factory.setPassword(_))

    factory
  })
  def this(uri: String) = this({
    val factory = new ConnectionFactory()
    factory.setUri(uri)
    factory
  })

  private lazy val connection = connectionFactory.newConnection()

  def createChannel(): Channel = new Channel(connection.createChannel())

  def close() = connection.close()
}
