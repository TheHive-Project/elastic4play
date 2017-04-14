package org.elastic4play.services

import java.io.IOException
import java.nio.file.{ FileVisitResult, Files, Path, Paths, SimpleFileVisitor }
import java.nio.file.attribute.BasicFileAttributes

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }

import akka.stream.Materializer

import play.api.Logger
import play.api.inject.ApplicationLifecycle
import play.api.mvc.{ Filter, RequestHeader, Result }

import org.elastic4play.utils.Instance

@Singleton
class TempSrv @Inject() (
    lifecycle: ApplicationLifecycle,
    implicit val ec: ExecutionContext) {

  lazy val log = Logger(getClass)

  private[TempSrv] val tempDir = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), "").resolve("play-request")
  lifecycle.addStopHook { () ⇒ Future { delete(tempDir) } }

  private[TempSrv] object deleteVisitor extends SimpleFileVisitor[Path] {
    override def visitFile(file: Path, attrs: BasicFileAttributes) = {
      Files.delete(file)
      FileVisitResult.CONTINUE
    }

    override def postVisitDirectory(dir: Path, e: IOException) = {
      Files.delete(dir)
      FileVisitResult.CONTINUE
    }
  }
  private[TempSrv] def delete(directory: Path): Unit = try {
    Files.walkFileTree(directory, deleteVisitor)
    ()
  }
  catch {
    case t: Throwable ⇒ log.warn(s"Fail to remove temporary files ($directory) : $t")
  }

  def newTemporaryFile(prefix: String, suffix: String)(implicit authContext: AuthContext) = {
    val td = tempDir.resolve(authContext.requestId)
    if (!Files.exists(td))
      Files.createDirectories(td)
    Files.createTempFile(tempDir.resolve(authContext.requestId), prefix, suffix)
  }

  def releaseTemporaryFiles()(implicit authContext: AuthContext): Unit = {
    releaseTemporaryFiles(authContext.requestId)
  }

  def releaseTemporaryFiles(request: RequestHeader): Unit = {
    releaseTemporaryFiles(Instance.getRequestId(request))
  }

  def releaseTemporaryFiles(requestId: String): Unit = {
    val d = tempDir.resolve(requestId)
    if (Files.exists(d))
      delete(d)
  }
}

class TempFilter @Inject() (
    tempSrv: TempSrv,
    implicit val ec: ExecutionContext,
    implicit val mat: Materializer) extends Filter {
  def apply(nextFilter: RequestHeader ⇒ Future[Result])(requestHeader: RequestHeader): Future[Result] = {
    nextFilter(requestHeader)
      .andThen { case _ ⇒ tempSrv.releaseTemporaryFiles(requestHeader) }
  }
}