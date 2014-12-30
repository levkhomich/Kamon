import akka.actor.ActorSystem

import scala.concurrent.ExecutionContext
import kamon.trace.TraceRecorder
import scala.concurrent.Future

package object test {
  def trace[T](name: String, category: Option[String] = None)(f: ⇒ T)(implicit sys: ActorSystem): T = {
    val parentToken = TraceRecorder.currentContext.token
    TraceRecorder.withNewTraceContext(name) {
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
    TraceRecorder.withNewTraceContext(name) {
      TraceRecorder.currentContext.addMetadata("parentToken", parentToken)
      category.foreach(TraceRecorder.currentContext.addMetadata("category", _))
      val future = f
      future.onComplete(_ ⇒ TraceRecorder.finish())
      future
    }
  }
}
