package kamon.zipkin

import akka.actor.{ ActorLogging, Actor }
import java.net.InetAddress
import java.nio.ByteBuffer
import scala.util.Random
import com.github.levkhomich.akka.tracing.TracingExtensionImpl
import com.github.levkhomich.akka.tracing.thrift
import kamon.metric.UserMetrics
import kamon.{ Kamon, NanoInterval, NanoTimestamp }
import kamon.trace.TraceInfo

class ZipkinActor(config: ZipkinConfig, tracingExt: TracingExtensionImpl) extends Actor with ActorLogging {
  import ZipkinActor._

  private implicit val ec = context.dispatcher
  private implicit val system = context.system

  val traceCount = Kamon(UserMetrics).registerCounter("trace-count")

  def receive = {
    case trace: TraceInfo ⇒
      traceCount.increment()
      tracingExt.submitSpans(traceInfoToSpans(trace))
  }

  private def timestampToMicros(nano: NanoTimestamp) = nano.nanos / 1000
  private def durationToMicros(nano: NanoInterval) = nano.nanos / 1000

  private def simpleSpan(traceId: Long, spanId: Long, name: String, start: Long, duration: Long,
    annotations: Map[String, String], parentSpanId: Long = 0,
    endpoint: thrift.Endpoint = createApplicationEndpoint(), isClient: Boolean = false) = {
    val sa = new thrift.Annotation()
    sa.set_timestamp(start)
    sa.set_value(if (isClient) thrift.zipkinConstants.CLIENT_SEND else thrift.zipkinConstants.SERVER_RECV)
    sa.set_host(endpoint)

    val ea = new thrift.Annotation()
    ea.set_timestamp(start + duration)
    ea.set_value(if (isClient) thrift.zipkinConstants.CLIENT_RECV else thrift.zipkinConstants.SERVER_SEND)
    ea.set_host(sa.get_host())

    val span = new thrift.Span()
    span.set_trace_id(traceId)
    span.set_id(spanId)
    span.set_parent_id(parentSpanId)
    span.set_name(name)
    span.add_to_annotations(sa)
    span.add_to_annotations(ea)
    annotations.foreach { case (k, v) ⇒ span.add_to_binary_annotations(stringAnnotation(k, v)) }
    span
  }

  private def longHash(string: String): Long = {
    var h = 1125899906842597L
    val len = string.length
    for (i ← 0 until len) {
      h = 31 * h + string.charAt(i)
    }
    h ^ sessionLong
  }

  private def traceInfoToSpans(trace: TraceInfo): List[com.github.levkhomich.akka.tracing.thrift.Span] = {
    val rootToken = trace.metadata.getOrElse(ZipkinTracing.rootToken, trace.token)
    val parentToken = trace.metadata.get(ZipkinTracing.parentToken)
    val token = trace.token

    val traceId = longHash(rootToken)
    val rootSpanId = longHash(token)
    val parentSpanId = parentToken.map(longHash).getOrElse(0L)

    val globalMetaData = Map("token" -> token)

    val cleanMetaData = trace.metadata.filterKeys(k ⇒ !k.startsWith(ZipkinTracing.internalPrefix))

    val (endpoint, isClient) = (trace.metadata.isDefinedAt(ZipkinTracing.clientServiceName) && trace.metadata.isDefinedAt(ZipkinTracing.clientServiceHost)) match {
      case true  ⇒ (createEndpoint(trace.metadata(ZipkinTracing.clientServiceName), trace.metadata(ZipkinTracing.clientServiceHost), trace.metadata.getOrElse(ZipkinTracing.clientServicePort, "0").toInt), true)
      case false ⇒ (createApplicationEndpoint(), false)
    }

    val root = simpleSpan(traceId, rootSpanId, trace.name, timestampToMicros(trace.timestamp), durationToMicros(trace.elapsedTime), globalMetaData ++ cleanMetaData, parentSpanId, endpoint, isClient)
    val children = trace.segments.map { segment ⇒
      val segmentAnnotations = Map(
        "category" -> segment.category,
        "library" -> segment.library)
      val cleanMetaData = segment.metadata.filterKeys(k ⇒ !k.startsWith(ZipkinTracing.internalPrefix))
      simpleSpan(traceId, Random.nextLong(), segment.name, timestampToMicros(segment.timestamp), durationToMicros(segment.elapsedTime), globalMetaData ++ segmentAnnotations ++ cleanMetaData, 0, endpoint, isClient)
    }
    root :: children
  }

  private def stringAnnotation(key: String, value: String) = {
    val a = new thrift.BinaryAnnotation()
    a.set_annotation_type(thrift.AnnotationType.STRING)
    a.set_key(key)
    a.set_value(ByteBuffer.wrap(value.getBytes))
    //    a.set_host(createApplicationEndpoint())
    a
  }

  private def createApplicationEndpoint() =
    createEndpoint(config.service.name, config.service.host, config.service.port)

  private def createEndpoint(service: String, host: String, port: Int): thrift.Endpoint =
    createEndpoint(service, InetAddress.getByName(host), port)

  private def createEndpoint(service: String, host: InetAddress, port: Int): thrift.Endpoint =
    createEndpoint(service, ByteBuffer.wrap(host.getAddress).getInt, port.toShort)

  private def createEndpoint(service: String, host: Int, port: Short): thrift.Endpoint = {
    val e = new thrift.Endpoint()
    e.set_service_name(service)
    e.set_ipv4(host)
    e.set_port(port)
    e
  }

}

object ZipkinActor {
  /**
   * to create unique trace and span IDs based on tokens, as tokens are only unique per application run,
   * a unique salt is used to generate global random IDs that are required by Zipkin.
   */
  private val sessionLong = 0

}