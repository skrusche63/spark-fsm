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

case class ServiceRequest(
  service:String,task:String,data:Map[String,String]
)
case class ServiceResponse(
  service:String,task:String,data:Map[String,String],status:String
)

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
  
  /*
   * Support for serialization and deserialization of rules
   */
  def serializeRules(rules:FSMRules):String = write(rules)
  
  def deserializeRules(rules:String):FSMRules = read[FSMRules](rules)
  
}

object Algorithms {
  
  val SPADE:String = "SPADE"
  val TSR:String   = "TSR"
  
}

object Sources {
  /* The names of the data source actually supported */
  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"    
  val PIWIK:String   = "PIWIK"    
}

object Messages {

  def ALGORITHM_IS_UNKNOWN(uid:String,algorithm:String):String = String.format("""Algorithm '%s' is unknown for uid '%s'.""", algorithm, uid)

  /*
   * Predict request have to provide either antecedents or consequents 
   * that will be used as match criteria against discovered rules
   */
  def NO_ANTECEDENTS_OR_CONSEQUENTS_PROVIDED(uid:String):String = 
    String.format("""
      No antecedents or consequents are provided for uid '%s'.
    """.stripMargin, uid)

  def MINING_STARTED(uid:String) = String.format("""Mining started for uid '%s'.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = String.format("""Parameters are missing for uid '%s'.""", uid)
 
  def NO_ALGORITHM_PROVIDED(uid:String):String = String.format("""No algorithm provided for uid '%s'.""", uid)

  def NO_PARAMETERS_PROVIDED(uid:String):String = String.format("""No parameters provided for uid '%s'.""", uid)

  def NO_SOURCE_PROVIDED(uid:String):String = String.format("""No source provided for uid '%s'.""", uid)

  def PATTERNS_DO_NOT_EXIST(uid:String):String = String.format("""The patterns for uid '%s' do not exist.""", uid)

  def RULES_DO_NOT_EXIST(uid:String):String = String.format("""The rules for uid '%s' do not exist.""", uid)

  def TASK_ALREADY_STARTED(uid:String):String = String.format("""The task with uid '%s' is already started.""", uid)

  def TASK_DOES_NOT_EXIST(uid:String):String = String.format("""The task with uid '%s' does not exist.""", uid)

  def TASK_IS_UNKNOWN(uid:String,task:String):String = String.format("""The task '%s' is unknown for uid '%s'.""", task, uid)

  def SOURCE_IS_UNKNOWN(uid:String,source:String):String = String.format("""Source '%s' is unknown for uid '%s'.""", source, uid)
  
}

object FSMStatus {
  
  val DATASET:String = "dataset"
    
  val STARTED:String = "started"
  val STOPPED:String = "stopped"
    
  val FINISHED:String = "finished"
  val RUNNING:String  = "running"
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}