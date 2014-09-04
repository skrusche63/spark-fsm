package de.kp.spark.fsm.actor
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
import akka.actor.{Actor,ActorLogging,ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.fsm.Configuration

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.util.JobCache

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class FSMMiner extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  private val algorithmSupport = Array(FSMAlgorithms.SPADE,FSMAlgorithms.TSR)
  
  def receive = {

    case req:FSMRequest => {
      
      val origin = sender    
      
      val (uid,task) = (req.uid,req.task)
      task match {
        
        case "start" => {
          
          val algorithm  = req.algorithm.getOrElse(null)
          val parameters = req.parameters.getOrElse(null)
          
          val source = req.source.getOrElse(null)
          val response = validateStart(uid,algorithm,parameters,source) match {
            
            case None => {
              /* Build job configuration */
              val jobConf = new JobConf()
                
              jobConf.set("uid",uid)
              jobConf.set("algorithm",algorithm)

              parameters.k match {
                case None => jobConf.set("k",10)
                case Some(k) => jobConf.set("k",k)
              }
               
              parameters.minconf match {
                case None => jobConf.set("minconf",0.9)
                case Some(minconf) => jobConf.set("minconf",minconf)
              }
               
              parameters.support match {
                case None => jobConf.set("support",0.5)
                case Some(support) => jobConf.set("support",support)
              }
              /* Start job */
              startJob(jobConf,source).mapTo[FSMResponse]
              
            }
            
            case Some(message) => {
              Future {new FSMResponse(uid,Some(message),None,None,None,FSMStatus.FAILURE)} 
              
            }
            
          }

          response.onSuccess {
            case result => origin ! FSMModel.serializeResponse(result)
          }

          response.onFailure {
            case message => {             
              val resp = new FSMResponse(uid,Some(message.toString),None,None,None,FSMStatus.FAILURE)
              origin ! FSMModel.serializeResponse(resp)	                  
            }	  
          }
         
        }
       
        case "status" => {
          /*
           * Job MUST exist the return actual status
           */
          val resp = if (JobCache.exists(uid) == false) {           
            val message = FSMMessages.TASK_DOES_NOT_EXIST(uid)
            new FSMResponse(uid,Some(message),None,None,None,FSMStatus.FAILURE)
            
          } else {            
            val status = JobCache.status(uid)
            new FSMResponse(uid,None,None,None,None,status)
            
          }
           
          origin ! FSMModel.serializeResponse(resp)
           
        }
        
        case _ => {
          
          val message = FSMMessages.TASK_IS_UNKNOWN(uid,task)
          val resp = new FSMResponse(uid,Some(message),None,None,None,FSMStatus.FAILURE)
           
          origin ! FSMModel.serializeResponse(resp)
            
        }
        
      }
      
    }
    
    case _ => {}
  
  }
  
  private def startJob(jobConf:JobConf,source:FSMSource):Future[Any] = {

    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second

    val algorithm = jobConf.get("algorithm").get.asInstanceOf[String]
    val actor = algorithmToActor(algorithm,jobConf)

    val path = source.path.getOrElse(null)
    if (path == null) {

      val req = new ElasticRequest()      
      ask(actor, req)
        
    } else {
    
      val req = new FileRequest(path)
      ask(actor, req)
        
    }
  
  }

  private def validateStart(uid:String,algorithm:String,parameters:FSMParameters,source:FSMSource):Option[String] = {

    if (JobCache.exists(uid)) {            
      val message = FSMMessages.TASK_ALREADY_STARTED(uid)
      return Some(message)
    
    }
            
    if (algorithm == null) {   
      val message = FSMMessages.NO_ALGORITHM_PROVIDED(uid)
      return Some(message)
    
    }
              
    if (algorithmSupport.contains(algorithm) == false) {
      val message = FSMMessages.ALGORITHM_IS_UNKNOWN(uid,algorithm)
      return Some(message)
    
    }
    
    if (parameters == null) {
      val message = FSMMessages.NO_PARAMETERS_PROVIDED(uid)
      return Some(message)
      
    }
    
    if (source == null) {
      val message = FSMMessages.NO_SOURCE_PROVIDED(uid)
      return Some(message)
 
    }

    None
    
  }

  private def algorithmToActor(algorithm:String,jobConf:JobConf):ActorRef = {

    val actor = if (algorithm == FSMAlgorithms.SPADE) {      
      context.actorOf(Props(new SPADEActor(jobConf)))      
      } else {
       context.actorOf(Props(new TSRActor(jobConf)))
      }
    
    actor
    
  }

}