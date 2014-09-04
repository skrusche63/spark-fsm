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

case class FSMRequest(
  /* 
   * Unique identifier to distinguish requests from each other;
   * the request is responsible for setting appropriate identifiers
   */
  uid:String,
  /*
   * The task of the request: for mining requests, two different task 
   * are supported:
   * 
   * a) start:   start a specific mining job
   * b) status:  get actual status of mining job
   */
  task:String,
  /*
   * The algorithm to be used when starting a specific mining job;
   * actually two different algorithms are supported: SPADE & TSR
   */
  algorithm:Option[String]
)

case class FSMResponse(
  /*
   * Unique identifier of the request, this response belongs to
   */
  uid:String,
  /*
   * Message
   */
  message:Option[String],
  /*
   * Patterns returned from SPADE
   */
  patterns:Option[String],
  /*
   * Rules returned from TSR
   */
  rules:Option[String],
  /*
   * Predictions returned from TSR
   */
  predictions:Option[String],
  /*
   * Return status
   */
  status:String
)

case class ElasticRequest()

case class FileRequest(
  path:String
)


object FSMModel {
    
  implicit val formats = Serialization.formats(NoTypeHints)

  def serializeResponse(response:FSMResponse):String = write(response)
  
  def deserializeRequest(request:String):FSMRequest = read[FSMRequest](request)
  
}

object FSMAlgorithms {
  
  val SPADE:String = "SPADE"
  val TSR:String   = "TSR"
  
}

object FSMMessages {

  def ANTECEDENTS_DO_NOT_EXIST(uid:String):String = String.format("""No antecedents found for uid '%s'.""", uid)

  def ALGORITHM_IS_UNKNOWN(uid:String,algorithm:String):String = String.format("""Algorithm '%s' is unknown for uid '%s'.""", algorithm, uid)
 
  def NO_ALGORITHM_PROVIDED(uid:String):String = String.format("""No algorithm provided for uid '%s'.""", uid)

  def NO_PARAMETERS_PROVIDED(uid:String):String = String.format("""No parameters provided for uid '%s'.""", uid)

  def NO_SOURCE_PROVIDED(uid:String):String = String.format("""No source provided for uid '%s'.""", uid)

  def RULES_DO_NOT_EXIST(uid:String):String = String.format("""The rules for uid '%s' do not exist.""", uid)

  def TASK_ALREADY_STARTED(uid:String):String = String.format("""The task with uid '%s' is already started.""", uid)

  def TASK_DOES_NOT_EXIST(uid:String):String = String.format("""The task with uid '%s' does not exist.""", uid)

  def TASK_IS_UNKNOWN(uid:String,task:String):String = String.format("""The task '%s' is unknown for uid '%s'.""", task, uid)
  
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