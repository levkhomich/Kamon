package kamon.zipkin

import akka.actor.ActorSystem
import kamon.trace.TraceRecorder

import scala.concurrent.{ ExecutionContext, Future }

case class ClientServiceData(name: String, host: String, port: Int)

object ZipkinTracing extends ZipkinTracing {
  val internalPrefix = "internal."

  val rootToken = "zipkin.rootToken"
  val parentToken = "zipkin.parentToken"

  val clientServiceName = "internal.zipkin.client.service.name"
  val clientServiceHost = "internal.zipkin.client.service.host"
  val clientServicePort = "internal.zipkin.client.service.port"
}

trait ZipkinTracing {
  def trace[T](name: String, client: Option[ClientServiceData] = None)(f: ⇒ T): T = {
    val parentToken = TraceRecorder.currentContext.token
    val rootToken = TraceRecorder.currentContext.metadata.getOrElse(ZipkinTracing.rootToken, parentToken)
    TraceRecorder.withNewTraceContext(name) {
      TraceRecorder.currentContext.addMetadata(ZipkinTracing.rootToken, rootToken)
      TraceRecorder.currentContext.addMetadata(ZipkinTracing.parentToken, parentToken)
      client.foreach { client ⇒
        TraceRecorder.currentContext.addMetadata(ZipkinTracing.clientServiceName, client.name)
        TraceRecorder.currentContext.addMetadata(ZipkinTracing.clientServiceHost, client.host)
        TraceRecorder.currentContext.addMetadata(ZipkinTracing.clientServicePort, client.port.toString)
      }
      try {
        f
      } finally {
        TraceRecorder.finish()
      }
    }(TraceRecorder.currentContext.system)
  }

  def traceFuture[T](name: String, client: Option[ClientServiceData] = None)(f: ⇒ Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val parentToken = TraceRecorder.currentContext.token
    val rootToken = TraceRecorder.currentContext.metadata.getOrElse(ZipkinTracing.rootToken, parentToken)
    TraceRecorder.withNewTraceContext(name) {
      TraceRecorder.currentContext.addMetadata(ZipkinTracing.rootToken, rootToken)
      TraceRecorder.currentContext.addMetadata(ZipkinTracing.parentToken, parentToken)
      client.foreach { client ⇒
        TraceRecorder.currentContext.addMetadata(ZipkinTracing.clientServiceName, client.name)
        TraceRecorder.currentContext.addMetadata(ZipkinTracing.clientServiceHost, client.host)
        TraceRecorder.currentContext.addMetadata(ZipkinTracing.clientServicePort, client.port.toString)
      }
      val future = f
      future.onComplete(_ ⇒ TraceRecorder.finish())
      future
    }(TraceRecorder.currentContext.system)
  }

  //  def trace[T](name: String, category: Option[String] = None)(f: ⇒ T)(implicit sys: ActorSystem): T = {
  //    val segment = TraceRecorder.currentContext.startSegment(name, category.getOrElse("unknown"), "unknown")
  //    try {
  //      f
  //    } finally {
  //      segment.finish()
  //    }
  //  }
  //
  //
  //  def traceFuture[T](name: String, category: Option[String] = None)(f: ⇒ Future[T])(implicit ec: ExecutionContext, sys: ActorSystem): Future[T] = {
  //    val segment = TraceRecorder.currentContext.startSegment(name, category.getOrElse("unknown"), "unknown")
  //    val future = f
  //    future.onComplete(_ => segment.finish())
  //    future
  //  }
}
