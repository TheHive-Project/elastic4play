package org.elastic4play.database

import javax.inject.{Inject, Singleton}

import scala.concurrent.{ExecutionContext, Future}

import com.sksamuel.elastic4s.ElasticDsl.update
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy

import org.elastic4play.models.{ModelAttributes, AttributeFormat ⇒ F, AttributeOption ⇒ O}

class SequenceModel extends ModelAttributes("sequence") {
  val counter = attribute("sequence", F.numberFmt, "Value of the sequence", O.model)
}

@Singleton
class DBSequence @Inject()(db: DBConfiguration, implicit val ec: ExecutionContext) {

  def apply(seqId: String): Future[Int] =
    db.execute {
      update(seqId)
        .in(db.indexName → "sequence")
        .upsert("counter" → 1)
        .script("ctx._source.counter += 1")
        .retryOnConflict(5)
        //.fetchSource(Seq("counter"), Nil) // doesn't work any longer
        .fetchSource(true)
        .refresh(RefreshPolicy.WAIT_UNTIL)
    } map { updateResponse ⇒
      updateResponse.get.sourceAsMap().get("counter").asInstanceOf[Int]
    }
}
