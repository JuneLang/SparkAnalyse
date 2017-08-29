package elastic4s

import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.searches.aggs.StatsAggregationDefinition

/**
 * Methods for aggregation query from ES.
 * Created by junlang on 8/22/17.
 */
class Aggregation(host: String, port: Int) {
  val client = HttpClient(ElasticsearchClientUri(host, port))

  def getAggregations(index: String,
                      qualiFields: Array[String] = Array.empty,
                      quantiFields: Array[String] = Array.empty) = {
    val result: SearchResponse = client.execute {
      search(index).aggregations{
        generateTermsAggregation(qualiFields) ++ generateStatAggregations(quantiFields)
      }
    }.await
    result.aggregationsAsString
  }

  /**
   * Get stats for quantitative columns.
   * @param index
   * @param fields
   * @return
   */
  def getStatAggregations(index: String,
                          fields: Array[String] = Array.empty) = {
    val result: SearchResponse = client.execute {
      search(index).aggregations{
        generateStatAggregations(fields)
      }
    }.await
    result.aggregationsAsMap.mapValues(v => {
      // v is a map
      v.asInstanceOf[Map[String, Double]]
    })
  }

  private def generateStatAggregations(fields: Array[String]): Array[StatsAggregationDefinition] = {
    fields.map(field => StatsAggregationDefinition(field, Some(field)))
  }

  def getTermsAggregation(index: String,
                          fields: Array[String] = Array.empty) = {
    val result: SearchResponse = client.execute {
      search(index).aggregations{
        generateTermsAggregation(fields)
      }
    }.await
    result.aggregationsAsMap.mapValues { case v =>
      // v is a map
      val map = v.asInstanceOf[Map[String, AnyRef]]
      val values = map.filter(x => x._1 == "buckets").mapValues( l => {
        // l is a list
        l.asInstanceOf[List[Map[String, AnyRef]]].toArray.map( m => {
          // m is a map
          (m("key").asInstanceOf[String], m("doc_count").asInstanceOf[Number].longValue())
        })
      })
      values("buckets")
    }
  }

  private def generateTermsAggregation(fields: Array[String]) = {
    fields.map(field => termsAgg(field, field).size(30))
  }

}
