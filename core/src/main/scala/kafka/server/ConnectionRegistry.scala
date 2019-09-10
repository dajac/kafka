package kafka.server

import java.net.InetAddress
import java.util
import java.util.concurrent.ConcurrentHashMap

import com.yammer.metrics.core.Counter
import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.security.auth.KafkaPrincipal

import scala.collection.JavaConverters._
import scala.collection.mutable

object ClientMetrics {
  val ConnectedClients = "ConnectedClients"
  val Connections = "Connections"
}

class ClientMetrics(val connections: ConcurrentHashMap[String, ConnectionMetadata]) extends KafkaMetricsGroup {
  import ClientMetrics._

  newGauge(ConnectedClients, new Gauge[Int] {
    def value: Int = {
      connections.size()
    }
  })

  newGauge(Connections, new Gauge[java.util.List[java.util.Map[String, String]]] {
    def value: java.util.List[java.util.Map[String, String]] = {
      val cons = new util.ArrayList[util.Map[String, String]]()

      connections.values().asScala.foreach { c =>
        cons.add(c.toMap())
      }

      cons
    }
  })

  private val metrics = mutable.Map[Map[String, String], Counter]()

  private def connectionTags(connectionMetadata: ConnectionMetadata): Map[String, String] =
    Map(
      "clientname" -> connectionMetadata.clientName,
      "clientversion" -> connectionMetadata.clientVersion
    )

  def incConnectedClients(connectionMetadata: ConnectionMetadata): Unit = {
    val tags = connectionTags(connectionMetadata)
    metrics.synchronized {
      metrics.get(tags) match {
        case Some(counter) => counter.inc()
        case None => {
          val counter = newCounter(Connections, tags)
          counter.inc()
          metrics(tags) = counter
        }
      }
    }
  }

  def decConnectionClients(connectionMetadata: ConnectionMetadata): Unit = {
    val tags = connectionTags(connectionMetadata)
    metrics.synchronized {
      metrics.get(tags) match {
        case Some(counter) => {
          counter.dec()
          if (counter.count() == 0) {
            removeMetric(Connections, tags)
            metrics.remove(tags)
          }
        }
        case None =>
      }
    }
  }

  def close(): Unit = {
    metrics.synchronized {
      metrics.foreach { case (tags, counter) =>
        removeMetric(Connections, tags)
      }
      metrics.clear()
    }
  }
}

object ConnectionMetadata {
  val UnknownClient = "Unknown"
  val UnknownVersion = "Unknown"

  def apply(clientId: String,
            clientAddress: InetAddress,
            principal: KafkaPrincipal,
            listenerName: String,
            securityProtocol: String): ConnectionMetadata = {
    ConnectionMetadata(clientId, UnknownClient, UnknownVersion, clientAddress, principal, listenerName, securityProtocol)
  }
}

case class ConnectionMetadata(clientId: String,
                              clientName: String,
                              clientVersion: String,
                              clientAddress: InetAddress,
                              principal: KafkaPrincipal,
                              listenerName: String,
                              securityProtocol: String) {

  // Keep it cached to avoid having to compute it every time
  private val map = new util.HashMap[String, String](7)

  Map(
    "ClientId" -> clientId,
    "ClientName" -> clientName,
    "ClientVersion" -> clientVersion,
    "ClientAddress" -> clientAddress.toString,
    "ListenerName" -> listenerName,
    "SecurityProtocol" -> securityProtocol
  ).foreach {case (k, v) => map.put(k, v) }

  def toMap(): util.Map[String, String] = {
    map
  }
}

class ConnectionRegistry {
  private val connections = new ConcurrentHashMap[String, ConnectionMetadata]
  private val metrics = new ClientMetrics(connections)

  def putConnection(connectionId: String, connectionMetadata: ConnectionMetadata): Unit = {
    val oldConnection = connections.put(connectionId, connectionMetadata)
    if (oldConnection != null)
      metrics.decConnectionClients(oldConnection)
    metrics.incConnectedClients(connectionMetadata)
  }

  def removeConnection(connectionId: String): Unit = {
    val connection = connections.remove(connectionId)
    if (connection != null)
      metrics.decConnectionClients(connection)
  }

  def getConnection(connectionId: String): Option[ConnectionMetadata] = {
    Option(connections.get(connectionId))
  }

  def close(): Unit = {
    connections.clear()
    metrics.close()
  }
}
