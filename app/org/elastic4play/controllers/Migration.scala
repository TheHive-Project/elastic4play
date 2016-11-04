package org.elastic4play.controllers

import javax.inject.{ Inject, Singleton }

import scala.annotation.implicitNotFound
import scala.concurrent.ExecutionContext

import play.api.mvc.{ Action, Controller }

import org.elastic4play.Timed
import org.elastic4play.services.MigrationSrv

/**
 * Migration controller : start migration process
 */
@Singleton
class MigrationCtrl @Inject() (migrationSrv: MigrationSrv,
                               implicit val ec: ExecutionContext) extends Controller {

  @Timed("controllers.MigrationCtrl.migrate")
  def migrate = Action.async {
    migrationSrv.migrate.map(_ => NoContent)
  }
}