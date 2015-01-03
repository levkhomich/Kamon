package kamon.zipkin

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

class ZipkinConfig(zipkinConfig: Config) {
  val enabled = zipkinConfig.getBoolean("enabled")
  object collector {
    private val config = zipkinConfig.getConfig("collector")

    val host = config.getString("host")
    val port = config.getInt("port")

    val flushInterval = config.getDuration("flushInterval", TimeUnit.MILLISECONDS)
    val maxFlushInterval = config.getDuration("maxFlushInterval", TimeUnit.MILLISECONDS)
    val maxScheduledSpans = config.getInt("maxScheduledSpans")
  }

  object service {
    private val config = zipkinConfig.getConfig("service")

    val name = config.getString("name")
    val host = config.getString("host") match {
      case "auto" ⇒ InetAddress.getLocalHost
      case host   ⇒ InetAddress.getByName(host)
    }
    val port = config.getInt("port")
  }
}
