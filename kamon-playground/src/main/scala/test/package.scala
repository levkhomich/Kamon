import akka.actor.ActorSystem

import scala.concurrent.ExecutionContext
import kamon.trace.TraceRecorder
import scala.concurrent.Future

package object test {
  def trace[T](name: String, category: Option[String] = None)(f: ⇒ T)(implicit sys: ActorSystem): T = {
    val parentToken = TraceRecorder.currentContext.token
    val rootToken = TraceRecorder.currentContext.metadata.getOrElse("rootToken", parentToken)
    TraceRecorder.withNewTraceContext(name) {
      TraceRecorder.currentContext.addMetadata("rootToken", rootToken)
      TraceRecorder.currentContext.addMetadata("parentToken", parentToken)
      category.foreach(TraceRecorder.currentContext.addMetadata("category", _))
      try {
        f
      } finally {
        TraceRecorder.finish()
      }
    }
  }

  def traceFuture[T](name: String, category: Option[String] = None)(f: ⇒ Future[T])(implicit ec: ExecutionContext, sys: ActorSystem): Future[T] = {
    val parentToken = TraceRecorder.currentContext.token
    val rootToken = TraceRecorder.currentContext.metadata.getOrElse("rootToken", parentToken)
    TraceRecorder.withNewTraceContext(name) {
      TraceRecorder.currentContext.addMetadata("rootToken", rootToken)
      TraceRecorder.currentContext.addMetadata("parentToken", parentToken)
      category.foreach(TraceRecorder.currentContext.addMetadata("category", _))
      val future = f
      future.onComplete(_ ⇒ TraceRecorder.finish())
      future
    }
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
