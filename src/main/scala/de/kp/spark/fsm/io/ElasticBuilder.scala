package de.kp.spark.fsm.io
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-FSM project
* (https://github.com/skrusche63/spark-fsm).
* 
* Spark-FSM is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-FSM is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-FSM. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

object ElasticBuilderFactory {
  /*
   * Definition of supported rule parameters
   */
  val TIMESTAMP_FIELD:String = "timestamp"

  /*
   * The unique identifier of the mining task that created the
   * respective rules
   */
  val UID_FIELD:String = "uid"
  
  /*
   * This is a relative identifier with respect to the timestamp
   * to specify which antecendents refer to the same association
   * rule
   */
  val RULE_FIELD:String = "rule"

    val ANTECEDENT_FIELD:String = "antecedent"
  val CONSEQUENT_FIELD:String = "consequent"

  val SUPPORT_FIELD:String = "support"
  val CONFIDENCE_FIELD:String = "confidence"
  
  def createBuilder(mapping:String):XContentBuilder = {
    /*
     * Define mapping schema for index 'index' and 'type'
     */
    val builder = XContentFactory.jsonBuilder()
                      .startObject()
                      .startObject(mapping)
                        .startObject("properties")

                          /* timestamp */
                          .startObject(TIMESTAMP_FIELD)
                            .field("type", "long")
                          .endObject()

                          /* uid */
                          .startObject(UID_FIELD)
                            .field("type", "string")
                            .field("index", "not_analyzed")
                          .endObject()
                    
                          /* rule */
                          .startObject(RULE_FIELD)
                            .field("type", "string")
                            .field("index", "not_analyzed")
                          .endObject()

                          /* antecedent */
                          .startObject(ANTECEDENT_FIELD)
                            .field("type", "integer")
                          .endObject()//

                          /* consequent */
                          .startObject(CONSEQUENT_FIELD)
                            .field("type", "integer")
                          .endObject()//

                          /* support */
                          .startObject(SUPPORT_FIELD)
                            .field("type", "integer")
                          .endObject()

                          /* confidence */
                          .startObject(CONFIDENCE_FIELD)
                            .field("type", "double")
                          .endObject()

                        .endObject() // properties
                      .endObject()   // mapping
                    .endObject()
                    
    builder

  }
  
}