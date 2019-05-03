package org.elastic4play.services

import javax.inject.{Inject, Singleton}

import scala.concurrent.ExecutionContext

import org.elastic4play.database.{DBConfiguration, DBSequence}

@Singleton
class SequenceSrv @Inject()(db: DBConfiguration, ec: ExecutionContext) extends DBSequence(db, ec)
