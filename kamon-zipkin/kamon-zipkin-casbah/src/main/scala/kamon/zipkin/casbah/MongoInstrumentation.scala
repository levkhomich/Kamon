package kamon.zipkin.casbah

import akka.actor.ActorSystem
import com.mongodb.casbah.MongoCollectionBase
import kamon.trace.{ TraceRecorder, TraceContextAware }
import kamon.zipkin.{ ClientServiceData, ZipkinTracing }
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation._

@Aspect
class MongoInstrumentation extends ZipkinTracing {
  private val mongoInfo = Some(ClientServiceData("mongodb", "localhost", 27017))

  @Around("execution(public * com.mongodb.casbah.MongoCollection.findOne(..)) && this(collection)")
  def aroundFindOneByID(pjp: ProceedingJoinPoint, collection: MongoCollectionBase): Any = {
    if (TraceRecorder.currentContext.isEmpty) {
      pjp.proceed()
    } else {
      trace(pjp.getSignature().getName(), mongoInfo) {
        TraceRecorder.currentContext.addMetadata("method", pjp.getSignature().toString())
        TraceRecorder.currentContext.addMetadata("args", pjp.getArgs().toList.mkString("; "))
        pjp.proceed()
      }
    }
  }

  @Around("execution(public * com.mongodb.casbah.MongoCollection.find(..)) && this(collection)")
  def aroundFind(pjp: ProceedingJoinPoint, collection: MongoCollectionBase): Any = {
    if (TraceRecorder.currentContext.isEmpty) {
      pjp.proceed()
    } else {
      trace(pjp.getSignature().getName(), mongoInfo) {
        TraceRecorder.currentContext.addMetadata("method", pjp.getSignature().toString())
        TraceRecorder.currentContext.addMetadata("args", pjp.getArgs().toList.mkString("; "))
        pjp.proceed()
      }
    }
  }

  @Around("execution(public * com.mongodb.casbah.MongoCollection.insert(..)) && this(collection)")
  def aroundInsert(pjp: ProceedingJoinPoint, collection: MongoCollectionBase): Any = {
    if (TraceRecorder.currentContext.isEmpty) {
      pjp.proceed()
    } else {
      trace(pjp.getSignature().getName(), mongoInfo) {
        TraceRecorder.currentContext.addMetadata("method", pjp.getSignature().toString())
        TraceRecorder.currentContext.addMetadata("args", pjp.getArgs().toList.mkString("; "))
        pjp.proceed()
      }
    }
  }

  @Around("execution(public * com.mongodb.casbah.MongoCollection.update(..)) && this(collection)")
  def aroundUpdate(pjp: ProceedingJoinPoint, collection: MongoCollectionBase): Any = {
    if (TraceRecorder.currentContext.isEmpty) {
      pjp.proceed()
    } else {
      trace(pjp.getSignature().getName(), mongoInfo) {
        TraceRecorder.currentContext.addMetadata("method", pjp.getSignature().toString())
        TraceRecorder.currentContext.addMetadata("args", pjp.getArgs().toList.mkString("; "))
        pjp.proceed()
      }
    }
  }

  @Around("execution(public * com.mongodb.casbah.MongoCollection.remove(..)) && this(collection)")
  def aroundRemove(pjp: ProceedingJoinPoint, collection: MongoCollectionBase): Any = {
    if (TraceRecorder.currentContext.isEmpty) {
      pjp.proceed()
    } else {
      trace(pjp.getSignature().getName(), mongoInfo) {
        TraceRecorder.currentContext.addMetadata("method", pjp.getSignature().toString())
        TraceRecorder.currentContext.addMetadata("args", pjp.getArgs().toList.mkString("; "))
        pjp.proceed()
      }
    }
  }
}
