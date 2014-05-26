package dst.amqp

import com.rabbitmq.client.AMQP.{BasicProperties => AMQPBasicProperties}
import java.sql.Date
import scala.collection.JavaConversions._
import scala.language.implicitConversions

case class BasicProperties(
  contentType:      Option[String]            = None,
  contentEncoding:  Option[String]            = None,
  headers:          Option[Map[String, Any]]  = None,
  deliveryMode:     Integer                   = BasicProperties.NonePersistent,
  priority:         Option[Integer]           = None,
  correlationId:    Option[String]            = None,
  replyTo:          Option[String]            = None,
  expiration:       Option[String]            = None,
  messageId:        Option[String]            = None,
  timestamp:        Option[Date]              = None,
  messageTypeName:  Option[String]            = None,
  userId:           Option[String]            = None,
  appId:            Option[String]            = None,
  clusterId:        Option[String]            = None
)

object BasicProperties {
  final val NonePersistent  = 1
  final val Persistent      = 2

  final val Empty = BasicProperties()
}

object BasicPropertiesImplicit {
  implicit def asJavaBasicProperties(value: BasicProperties): AMQPBasicProperties = {
    val headers = value.headers match {
      case Some(headers) => mapAsJavaMap(headers).asInstanceOf[java.util.Map[java.lang.String, java.lang.Object]]
      case None => null
    }
    new AMQPBasicProperties(
      value.contentType.getOrElse(null),
      value.contentEncoding.getOrElse(null),
      headers,
      value.deliveryMode,
      value.priority.getOrElse(null),
      value.correlationId.getOrElse(null),
      value.replyTo.getOrElse(null),
      value.expiration.getOrElse(null),
      value.messageId.getOrElse(null),
      value.timestamp.getOrElse(null),
      value.messageTypeName.getOrElse(null),
      value.userId.getOrElse(null),
      value.appId.getOrElse(null),
      value.clusterId.getOrElse(null)
    )
  }
}
