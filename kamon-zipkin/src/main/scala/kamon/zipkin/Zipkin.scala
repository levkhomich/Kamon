package kamon.zipkin

import akka.actor._
import akka.event.Logging
import com.github.levkhomich.akka.tracing.TracingExtension
import kamon.Kamon
import kamon.metric.{ MetricIdentity, MetricGroupIdentity }
import kamon.trace.Trace

object Zipkin extends ExtensionId[ZipkinExtension] with ExtensionIdProvider {
  override def lookup(): ExtensionId[_ <: Extension] = Zipkin
  override def createExtension(system: ExtendedActorSystem): ZipkinExtension = new ZipkinExtension(system)

  trait MetricKeyGenerator {
    def generateKey(groupIdentity: MetricGroupIdentity, metricIdentity: MetricIdentity): String
  }
}

class ZipkinExtension(system: ExtendedActorSystem) extends Kamon.Extension {
  val log = Logging(system, classOf[ZipkinExtension])
  log.info("Starting the Kamon(Zipkin) extension")
  log.debug(s"Kamon(Zipkin) config dump:\n${system.settings.config.getConfig("kamon.zipkin")}")

  if (system.settings.config.getBoolean("akka.tracing.enabled")) {
    val config = new ZipkinConfig(system.settings.config.getConfig("kamon.zipkin"))
    val trace = TracingExtension(system)
    log.debug("Registering Kamon(Zipkin) actors")
    val zipkinActor = system.actorOf(Props(new ZipkinActor(config, trace)))
    Kamon(Trace)(system).subscribe(zipkinActor)
  } else {
    log.warning("Kamon(Zipkin) extension disabled")
  }
}

