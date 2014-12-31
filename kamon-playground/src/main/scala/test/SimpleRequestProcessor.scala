/* ===================================================
 * Copyright © 2013 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ========================================================== */

package test

import java.net.InetAddress
import java.nio.ByteBuffer
import javax.xml.bind.DatatypeConverter

import akka.actor._
import akka.routing.RoundRobinPool
import akka.util.Timeout
import kamon.{NanoInterval, NanoTimestamp, Kamon}
import kamon.metric.Subscriptions.TickMetricSnapshot
import kamon.metric._
import kamon.spray.KamonTraceDirectives
import kamon.trace.{TraceInfo, Trace, SegmentCategory, TraceRecorder}
import kamon.zipkin.TReusableTransport
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TFramedTransport}
import spray.http.{ StatusCodes, Uri }
import spray.httpx.RequestBuilding
import spray.routing.SimpleRoutingApp
import kamon.zipkin.thrift

import scala.concurrent.{ Await, Future }
import scala.util.Random

object ZipKinActor {
  val sessionLong = Random.nextLong()
}

class ZipKinActor extends Actor {
  private val localAddress = ByteBuffer.wrap(InetAddress.getLocalHost.getAddress).getInt

  private val protocolFactory = new TBinaryProtocol.Factory()
  private val thriftBuffer = new TReusableTransport()

  val transport = new TFramedTransport(
    new TSocket("localhost", 9410)
  )
  private val client = new thrift.Scribe.Client(new TBinaryProtocol(transport))


  def receive = {
    case trace: TraceInfo ⇒
      import scala.collection.JavaConversions._

      val spans = traceInfoToSpans(trace)
      transport.open()
      client.Log(spans.map(logEntryFromSpan))
      transport.close()
  }

  private def timestampToMicros(nano: NanoTimestamp) = nano.nanos / 1000
  private def durationToMicros(nano: NanoInterval) = nano.nanos / 1000


  private def simpleSpan(traceId: Long, spanId: Long, name: String, start: Long, duration: Long, annotations: Map[String, String], parentSpanId: Long = 0) = {
    val sr = new thrift.Annotation()
    sr.set_timestamp(start)
    sr.set_value(thrift.zipkinConstants.SERVER_RECV)
    sr.set_host(createEndpoint())

    val ss = new thrift.Annotation()
    ss.set_timestamp(start + duration)
    ss.set_value(thrift.zipkinConstants.SERVER_SEND)
    ss.set_host(sr.get_host())

    val span = new thrift.Span()
    span.set_trace_id(traceId)
    span.set_id(spanId)
    span.set_parent_id(parentSpanId)
    span.set_name(name)
    span.add_to_annotations(sr)
    span.add_to_annotations(ss)
    annotations.foreach { case (k, v) => span.add_to_binary_annotations(stringAnnotation(k, v)) }
    span
  }

  def longHash(string: String): Long = {
    var h = 1125899906842597L
    val len = string.length
    for (i <- 0 until len) {
      h = 31 * h + string.charAt(i)
    }
    h ^ ZipKinActor.sessionLong
  }

  private def traceInfoToSpans(trace: TraceInfo) = {
    val rootToken = trace.metadata.getOrElse("rootToken", trace.token)
    val parentToken = trace.metadata.get("parentToken")
    val token = trace.token

    val globalAnnotations = Map(
      "token" -> token,
      "rootToken" -> rootToken
    ) ++ (parentToken match {
      case Some(parentToken) => Map("parentToken" -> parentToken)
      case None => Map.empty
    })

    val traceId = longHash(rootToken)
    val rootSpanId = longHash(token)
    val parentSpanId = parentToken.map(longHash).getOrElse(0L)

    val root = simpleSpan(traceId, rootSpanId, trace.name, timestampToMicros(trace.timestamp), durationToMicros(trace.elapsedTime), globalAnnotations ++ trace.metadata, parentSpanId)
    val children = trace.segments.map { segment =>
      val segmentAnnotations = Map(
        "category" -> segment.category,
        "library" -> segment.library
      )
      simpleSpan(traceId, Random.nextLong(), segment.name, timestampToMicros(segment.timestamp), durationToMicros(segment.elapsedTime), globalAnnotations ++ segmentAnnotations ++ trace.metadata)
    }
    root :: children
  }

  private def stringAnnotation(key: String, value: String) = {
    val a = new thrift.BinaryAnnotation()
    a.set_annotation_type(thrift.AnnotationType.STRING)
    a.set_key(key)
    a.set_value(ByteBuffer.wrap(value.getBytes))
    a.set_host(createEndpoint())
    a
  }


  private def createEndpoint(service: String = "kamon") = {
    val e = new thrift.Endpoint()
    e.set_ipv4(localAddress)
    e.set_port(0)
    e.set_service_name(service)
    e
  }

  private def logEntryFromSpan(span: thrift.Span): thrift.LogEntry = {
    span.write(protocolFactory.getProtocol(thriftBuffer))
    val thriftBytes = thriftBuffer.getArray.take(thriftBuffer.length)
    thriftBuffer.reset()
    val encodedSpan = DatatypeConverter.printBase64Binary(thriftBytes) + '\n'
    new thrift.LogEntry("zipkin", encodedSpan)
  }
}

object SimpleRequestProcessor extends App with SimpleRoutingApp with RequestBuilding with KamonTraceDirectives {
  import akka.pattern.ask
  import spray.client.pipelining._

  import scala.concurrent.duration._

  implicit val system = ActorSystem("test")
  import test.SimpleRequestProcessor.system.dispatcher

  val printer = system.actorOf(Props[PrintWhatever])
  val zipKinActor = system.actorOf(Props[ZipKinActor])

  val act = system.actorOf(Props(new Actor {
    def receive: Actor.Receive = { case any ⇒ sender ! any }
  }), "com")

  Kamon(Trace).subscribe(printer)
  Kamon(Trace).subscribe(zipKinActor)
  //val buffer = system.actorOf(TickMetricSnapshotBuffer.props(30 seconds, printer))

  //Kamon(Metrics).subscribe(CustomMetric, "*", buffer, permanently = true)
  //Kamon(Metrics).subscribe(ActorMetrics, "*", printer, permanently = true)

  implicit val timeout = Timeout(30 seconds)

  val counter = Kamon(UserMetrics).registerCounter("requests")
  Kamon(UserMetrics).registerCounter("requests-2")
  Kamon(UserMetrics).registerCounter("requests-3")

  Kamon(UserMetrics).registerHistogram("histogram-1")
  Kamon(UserMetrics).registerHistogram("histogram-2")

  Kamon(UserMetrics).registerMinMaxCounter("min-max-counter-1")
  Kamon(UserMetrics).registerMinMaxCounter("min-max-counter-2")
  Kamon(UserMetrics).registerMinMaxCounter("min-max-counter-3")

  //Kamon(UserMetrics).registerGauge("test-gauge")(() => 10L)

  val pipeline = sendReceive
  val replier = system.actorOf(Props[Replier].withRouter(RoundRobinPool(nrOfInstances = 4)), "replier")
  val asyncWork = system.actorOf(Props(new AsyncWork()))

  val random = new Random()

  startServer(interface = "localhost", port = 9090) {
    get {
      path("foobar") {
        get {
          complete {
            val f1 = {
              traceFuture("f1") {
                Future {
                  Thread.sleep(500)
                  println("f1", TraceRecorder.currentContext.token)
                  asyncWork ! "ping"
                  "Hello Kamon"
                }
              }
            }
            val f2 = Future {
              trace("f2") {
                Thread.sleep(1000)
                println("f2", TraceRecorder.currentContext.token)
                "OK"
              }
            }
            Future.sequence(List(f1, f2)).map { strList ⇒
              trace("combine") {
                println("fseq", TraceRecorder.currentContext.token)
                strList.mkString(" ")
              }
            }
          }
        }
      } ~
        path("test") {
          traceName("test") {
            complete {
              val futures = pipeline(Get("http://10.254.209.14:8000/")).map(r ⇒ "Ok") :: pipeline(Get("http://10.254.209.14:8000/")).map(r ⇒ "Ok") :: Nil

              Future.sequence(futures).map(l ⇒ "Ok")
            }
          }
        } ~
        path("site") {
          traceName("FinalGetSite-3") {
            complete {
              for (
                f1 ← pipeline(Get("http://127.0.0.1:9090/ok"));
                f2 ← pipeline(Get("http://www.google.com/search?q=mkyong"))
              ) yield "Ok Double Future"
            }
          }
        } ~
        path("site-redirect") {
          redirect(Uri("http://localhost:4000/"), StatusCodes.MovedPermanently)

        } ~
        path("reply" / Segment) { reqID ⇒
          traceName("reply") {
            complete {
              (replier ? reqID).mapTo[String]
            }
          }
        } ~
        path("ok") {
          traceName("RespondWithOK-3") {
            complete {
              "ok"
            }
          }
        } ~
        path("future") {
          traceName("OKFuture") {
            dynamic {
              counter.increment()
              complete(Future { "OK" })
            }
          }
        } ~
        path("kill") {
          dynamic {
            replier ! PoisonPill
            complete(Future { "OK" })
          }
        } ~
        path("error") {
          complete {
            throw new NullPointerException
            "okk"
          }
        } ~
        path("segment") {
          complete {
            val segment = TraceRecorder.currentContext.startSegment("hello-world", SegmentCategory.HttpClient, "none")
            (replier ? "hello").mapTo[String].onComplete { t ⇒
              segment.finish()
            }

            "segment"
          }
        }
    }
  }

}

class PrintWhatever extends Actor {
  def receive = {
    case TickMetricSnapshot(from, to, metrics) ⇒
      println(metrics.map { case (key, value) ⇒ key.name + " => " + value.metrics.mkString(",") }.mkString("|"))
    case anything ⇒ println(anything)
  }
}

object Verifier extends App {

  def go: Unit = {
    import spray.client.pipelining._

    import scala.concurrent.duration._

    implicit val system = ActorSystem("test")
    import system.dispatcher

    implicit val timeout = Timeout(30 seconds)

    val pipeline = sendReceive

    val futures = Future.sequence(for (i ← 1 to 500) yield {
      pipeline(Get("http://127.0.0.1:9090/reply/" + i)).map(r ⇒ r.entity.asString == i.toString)
    })
    println("Everything is: " + Await.result(futures, 10 seconds).forall(a ⇒ a == true))
  }


}

class Replier extends Actor with ActorLogging {
  def receive = {
    case anything ⇒
      if (TraceRecorder.currentContext.isEmpty)
        log.warning("PROCESSING A MESSAGE WITHOUT CONTEXT")

      //log.info("Processing at the Replier, and self is: {}", self)
      sender ! anything
  }
}

class AsyncWork extends Actor with ActorLogging {
  import context.system

  def receive = {
    case anything ⇒
      if (TraceRecorder.currentContext.isEmpty)
        log.warning("PROCESSING A MESSAGE WITHOUT CONTEXT")

      trace("async") {
        println(anything)

        println("async", TraceRecorder.currentContext.token)
      }
  }
}

object PingPong extends App {
  val system = ActorSystem()
  val pinger = system.actorOf(Props(new Actor {
    def receive: Actor.Receive = { case "pong" ⇒ sender ! "ping" }
  }))
  val ponger = system.actorOf(Props(new Actor {
    def receive: Actor.Receive = { case "ping" ⇒ sender ! "pong" }
  }))

  pinger.tell("pong", ponger)

}
