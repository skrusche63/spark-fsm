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

case class Listener(
  timeout:Int, url:String
)
/**
 * ServiceRequest & ServiceResponse specify the content 
 * sent to and received from the decision service
 */
case class ServiceRequest(
  service:String,task:String,data:Map[String,String]
)
case class ServiceResponse(
  service:String,task:String,data:Map[String,String],status:String
)
/*
 * The Field and Fields classes are used to specify the fields with
 * respect to the data source provided, that have to be mapped onto
 * site,timestamp,user,group,item
 */
case class Field(
  name:String,datatype:String,value:String
)
case class Fields(items:List[Field])

/*
 * Service requests are mapped onto job descriptions and are stored
 * in a Redis instance
 */
case class JobDesc(
  service:String,task:String,status:String
)

case class FSMPattern(
  support:Int,itemsets:List[List[Int]])

case class FSMPatterns(items:List[FSMPattern])
  

case class FSMRule (
  antecedent:List[Int],consequent:List[Int],support:Int,confidence:Double)

case class FSMRules(items:List[FSMRule])

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def serializeFields(fields:Fields):String = write(fields)
  
  def deserializeFields(fields:String):Fields = read[Fields](fields)

  /*
   * Support for serialization and deserialization of job descriptions
   */
  def serializeJob(job:JobDesc):String = write(job)

  def deserializeJob(job:String):JobDesc = read[JobDesc](job)
  /*
   * Support for serialization and deserialization of patterns
   */
  def serializePatterns(patterns:FSMPatterns):String = write(patterns)
  
  def deserializePatterns(patterns:String):FSMPatterns = read[FSMPatterns](patterns)

  def serializeResponse(response:ServiceResponse):String = write(response)
  
  def deserializeRequest(request:String):ServiceRequest = read[ServiceRequest](request)
  def serializeRequest(request:ServiceRequest):String = write(request)
  
  /*
   * Support for serialization and deserialization of rules
   */
  def serializeRules(rules:FSMRules):String = write(rules)
  
  def deserializeRules(rules:String):FSMRules = read[FSMRules](rules)
  
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
  val PIWIK:String   = "PIWIK"    

  private val sources = List(FILE,ELASTIC,JDBC,PIWIK)
  def isSource(source:String):Boolean = sources.contains(source)
  
}

object Sinks {
  
  val ELASTIC:String = "ELASTIC"
  val JDBC:String    = "JDBC"
    
  private val sinks = List(ELASTIC,JDBC)
  def isSink(sink:String):Boolean = sinks.contains(sink)
  
}

object Messages {

  def ALGORITHM_IS_UNKNOWN(uid:String,algorithm:String):String = 
    String.format("""[UID: %s] Algorithm '%s' is unknown.""", uid, algorithm)

  def GENERAL_ERROR(uid:String):String = 
    String.format("""[UID: %s] A general error appeared.""", uid)

  def MINING_STARTED(uid:String) = 
    String.format("""[UID: %s] Training task started.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = 
    String.format("""[UID: %s] Training task has missing parameters.""", uid)
 
  def NO_ALGORITHM_PROVIDED(uid:String):String = 
    String.format("""[UID: %s] No algorithm specified.""", uid)

  def NO_ITEMS_PROVIDED(uid:String):String = 
    String.format("""[UID: %s] No items are provided.""", uid)

  def NO_PARAMETERS_PROVIDED(uid:String):String = 
    String.format("""[UID: %s] No parameters provided.""", uid)

  def NO_SOURCE_PROVIDED(uid:String):String = 
    String.format("""[UID: %s] No source provided.""", uid)

  def PATTERNS_DO_NOT_EXIST(uid:String):String = 
    String.format("""[UID: %s] No patterns found.""", uid)

  def REQUEST_IS_UNKNOWN():String = String.format("""Unknown request.""")

  def RULES_DO_NOT_EXIST(uid:String):String = 
    String.format("""[UID: %s] No association rules found.""", uid)

  def SEARCH_INDEX_CREATED(uid:String):String = 
    String.format("""[UID: %s] Search index created.""", uid)

  def SOURCE_IS_UNKNOWN(uid:String,source:String):String = 
    String.format("""[UID: %s] Data source '%s' is unknown.""", uid, source)

  def TASK_ALREADY_STARTED(uid:String):String = 
    String.format("""[UID: %s] The task has already started.""", uid)

  def TASK_DOES_NOT_EXIST(uid:String):String = 
    String.format("""[UID: %s] The task does not exist.""", uid)

  def TASK_IS_UNKNOWN(uid:String,task:String):String = 
    String.format("""[UID: %s] The task '%s' is unknown.""", uid, task)
 
  def TRACKED_ITEM_RECEIVED(uid:String):String = 
    String.format("""[UID: %s] Tracked item received.""", uid)
  
}

object ResponseStatus {
  
  val DATASET:String = "dataset"
    
  val STARTED:String = "started"
  val STOPPED:String = "stopped"
    
  val FINISHED:String = "finished"
  val RUNNING:String  = "running"
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}