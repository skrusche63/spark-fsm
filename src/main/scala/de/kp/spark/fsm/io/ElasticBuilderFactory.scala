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

import org.elasticsearch.common.xcontent.XContentBuilder
import de.kp.spark.core.elastic.ElasticItemBuilder

object ElasticBuilderFactory {
  /*
   * Definition of supported common parameters
   */
  val TIMESTAMP_FIELD:String = "timestamp"

  /******************************************************************
   *                          RULE
   *****************************************************************/

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

  /******************************************************************
   *                          ITEM
   *****************************************************************/

  val SITE_FIELD:String = "site"
  val USER_FIELD:String = "user"

  val GROUP_FIELD:String = "group"
  val ITEM_FIELD:String  = "item"

  def getBuilder(builder:String,mapping:String):XContentBuilder = {
    
    builder match {
      /*
       * Elasticsearch is used to track item-based events to provide
       * a transaction database within an Elasticsearch index
       */
      case "item" => new ElasticItemBuilder().createBuilder(mapping)
      /*
       * Elasticsearch is also used as a rule sink to enable seamless
       * usage of the association rule mining results
       */
      case "rule" => new ElasticRuleBuilder().createBuilder(mapping)
      
      case _ => null
      
    }
  
  }
  
}