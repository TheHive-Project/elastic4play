package org.elastic4play.database

import com.sksamuel.elastic4s.RefreshPolicy
import javax.inject.{Inject, Singleton}

import scala.concurrent.{ExecutionContext, Future}
import com.sksamuel.elastic4s.http.ElasticDsl._
import org.elastic4play.models.{ModelAttributes, AttributeFormat => F, AttributeOption => O}

class SequenceModel extends ModelAttributes("sequence") {
  val counter = attribute("sequenceCounter", F.numberFmt, "Value of the sequence", O.model)
}

@Singleton
class DBSequence @Inject()(db: DBConfiguration) {

  def apply(seqId: String)(implicit ec: ExecutionContext): Future[Int] =
    db.execute {
      update(s"sequence_$seqId")
        .in(db.indexName / "doc")
        .upsert("sequenceCounter" -> 1, "relations" -> "sequence")
        .script("ctx._source.sequenceCounter += 1")
        .retryOnConflict(5)
        .fetchSource(true)
        .refresh(RefreshPolicy.WAIT_UNTIL)
    } map { updateResponse =>
      updateResponse.source("sequenceCounter").asInstanceOf[Int]
    }
}
