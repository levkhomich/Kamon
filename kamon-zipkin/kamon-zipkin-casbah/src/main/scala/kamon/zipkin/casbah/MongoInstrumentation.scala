package kamon.zipkin.casbah

import akka.actor.ActorSystem
import com.mongodb.casbah.MongoCollectionBase
import kamon.trace.{TraceRecorder, TraceContextAware}
import kamon.zipkin.{ClientServiceData, ZipkinTracing}
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation._

@Aspect
class MongoInstrumentation extends ZipkinTracing {
  private val mongoInfo = Some(ClientServiceData("mongodb", "localhost", 27017))

  @Around("execution(public * com.mongodb.casbah.MongoCollection.find*(..)) && this(collection)")
  def aroundFindOneByID(pjp: ProceedingJoinPoint, collection: MongoCollectionBase): Any = {
    trace(pjp.getSignature().getName(), mongoInfo) {
      TraceRecorder.currentContext.addMetadata("method", pjp.getSignature().toString())
      TraceRecorder.currentContext.addMetadata("args", pjp.getArgs().toList.mkString("; "))
      pjp.proceed()
    }
  }
}
