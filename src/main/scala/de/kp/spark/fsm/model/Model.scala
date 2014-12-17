package de.kp.spark.fsm.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import de.kp.spark.core.model._

case class FSMPattern(
  support:Int,itemsets:List[List[Int]])

case class FSMPatterns(items:List[FSMPattern])

object Serializer extends BaseSerializer {
    
  /*
   * Support for serialization and deserialization of patterns
   */
  def serializePatterns(patterns:FSMPatterns):String = write(patterns)  
  def deserializePatterns(patterns:String):FSMPatterns = read[FSMPatterns](patterns)
  
}

object Algorithms {
  
  val SPADE:String = "SPADE"
  val TSR:String   = "TSR"
  
  private val algorithms = List(SPADE,TSR)
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
  
}

object Sources {

  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"    
  val PARQUET:String = "PARQUET"
  val PIWIK:String   = "PIWIK"    

  private val sources = List(FILE,ELASTIC,JDBC,PARQUET,PIWIK)
  def isSource(source:String):Boolean = sources.contains(source)
  
}

object Sinks {
  
  val ELASTIC:String = "ELASTIC"
  val JDBC:String    = "JDBC"
    
  private val sinks = List(ELASTIC,JDBC)
  def isSink(sink:String):Boolean = sinks.contains(sink)
  
}

object Messages extends BaseMessages {

  def MINING_STARTED(uid:String) = 
    String.format("""[UID: %s] Training task started.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = 
    String.format("""[UID: %s] Training task has missing parameters.""", uid)

  def NO_ITEMS_PROVIDED(uid:String):String = 
    String.format("""[UID: %s] No items are provided.""", uid)

  def PATTERNS_DO_NOT_EXIST(uid:String):String = 
    String.format("""[UID: %s] No patterns found.""", uid)
  
}

object ResponseStatus extends BaseStatus