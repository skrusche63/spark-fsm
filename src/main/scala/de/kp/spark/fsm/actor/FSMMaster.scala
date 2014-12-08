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

import org.apache.spark.SparkContext

import akka.actor.{ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import akka.actor.{OneForOneStrategy, SupervisorStrategy}

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.fsm.Configuration
import de.kp.spark.fsm.model._

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

class FSMMaster(@transient sc:SparkContext) extends BaseActor {
  
  /* Load configuration for routers */
  val (duration,retries,time) = Configuration.actor   
      
  implicit val ec = context.dispatcher
  implicit val timeout:Timeout = DurationInt(time).second

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(duration).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }
  
  def receive = {
    
    case req:String => {
	  	    
	  val origin = sender

	  val deser = Serializer.deserializeRequest(req)
	  val response = execute(deser)
	  
      response.onSuccess {
        case result => origin ! serialize(result)
      }
      response.onFailure {
        case result => origin ! serialize(failure(deser,Messages.GENERAL_ERROR(deser.data("uid"))))	      
	  }
      
    }
    
    case req:ServiceRequest => {
	  	    
	  val origin = sender

	  val response = execute(req)
      response.onSuccess {
        case result => origin ! result
      }
      response.onFailure {
        case result => origin ! failure(req,Messages.GENERAL_ERROR(req.data("uid")))	      
	  }
      
    }
  
    case _ => {
 
      val msg = Messages.REQUEST_IS_UNKNOWN()          
      log.error(msg)
     
    }
    
  }

  private def execute(req:ServiceRequest):Future[ServiceResponse] = {
	
    try {
      
      val task = req.task.split(":")(0)
      ask(actor(task),req).mapTo[ServiceResponse]
    
    } catch {
      
      case e:Exception => {
        Future {failure(req,e.getMessage)}         
      }
    
    }
    
  }
  
  private def actor(worker:String):ActorRef = {
    
    worker match {
      /*
       * Metadata management is part of the core functionality; field or metadata
       * specifications can be registered in, and retrieved from a Redis database.
       */
      case "fields"   => context.actorOf(Props(new FieldQuestor(Configuration)))
      case "register" => context.actorOf(Props(new BaseRegistrar(Configuration)))        
      /*
       * Index management is part of the core functionality; an Elasticsearch 
       * index can be created and appropriate (tracked) items can be saved.
       */  
      case "index" => context.actorOf(Props(new BaseIndexer(Configuration)))
      case "track" => context.actorOf(Props(new BaseTracker(Configuration)))
      /*
       * Status management is part of the core functionality and comprises the
       * retrieval of the stati of a certain data mining or model building task
       */        
      case "status" => context.actorOf(Props(new StatusQuestor(Configuration)))
  
      case "miner" => context.actorOf(Props(new FSMMiner(sc)))
      case "get"   => context.actorOf(Props(new FSMQuestor()))
      
      case _ => null
      
    }
  
  }

}
