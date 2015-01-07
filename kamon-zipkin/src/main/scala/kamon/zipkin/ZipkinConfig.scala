package kamon.zipkin

import java.net.InetAddress

import com.typesafe.config.Config

class ZipkinConfig(zipkinConfig: Config) {
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
