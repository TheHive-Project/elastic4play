package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import com.sksamuel.elastic4s.http.ElasticDsl.updateById
import org.elastic4play.models.{ ModelAttributes, AttributeFormat ⇒ F, AttributeOption ⇒ O }

import scala.concurrent.{ ExecutionContext, Future }

class SequenceModel extends ModelAttributes("sequence") {
  val counter = attribute("sequence", F.numberFmt, "Value of the sequence", O.model)
}

@Singleton
class DBSequence @Inject() (
    db: DBConfiguration,
    implicit val ec: ExecutionContext) {

  def apply(seqId: String): Future[Int] = {
    db.execute {
      updateById(db.indexName, "sequence", seqId)
        .upsert("counter" → 1)
        .script("ctx._source.counter += 1")
        .retryOnConflict(5)
        //.fetchSource(Seq("counter"), Nil) // doesn't work any longer
        .fetchSource(true)
        .refreshImmediately // FIXME .refresh(RefreshPolicy.WAIT_UNTIL)
    } map { updateResponse ⇒
      (DBUtils.toJson(updateResponse.source) \ "counter").as[Int]
    }
  }
}