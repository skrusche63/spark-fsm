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

import akka.actor.{OneForOneStrategy, SupervisorStrategy}

import de.kp.spark.fsm.Configuration
import de.kp.spark.fsm.model._

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

class FSMMaster extends Actor with ActorLogging with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("FSMContext",Configuration.spark)      
  
  /* Load configuration for routers */
  val (duration,retries,time) = Configuration.actor   

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(duration).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }
  
  def receive = {
    
    case req:String => {
      
      implicit val ec = context.dispatcher
      implicit val timeout:Timeout = DurationInt(time).second
	  	    
	  val origin = sender

	  val deser = Serializer.deserializeRequest(req)
	  val response = deser.task match {
        
        case "train"  => ask(actor("miner"),deser).mapTo[ServiceResponse]
        case "status" => ask(actor("miner"),deser).mapTo[ServiceResponse]
        
        case "rules" => ask(actor("questor"),deser).mapTo[ServiceResponse]
        case "patterns" => ask(actor("questor"),deser).mapTo[ServiceResponse]

        case "predict" => ask(actor("questor"),deser).mapTo[ServiceResponse]
       
        case _ => {

          Future {          
            failure(deser,Messages.TASK_IS_UNKNOWN(deser.data("uid"),deser.task))
          } 
          
        }
      
      }
      response.onSuccess {
        case result => origin ! Serializer.serializeResponse(result)
      }
      response.onFailure {
        case result => origin ! FSMStatus.FAILURE	      
	  }
      
    }
  
    case _ => {}
    
  }

  private def actor(worker:String):ActorRef = {
    
    worker match {
  
      case "miner" => context.actorOf(Props(new FSMMiner(sc)))
        
      case "questor" => context.actorOf(Props(new FSMQuestor()))
      
      case _ => null
      
    }
  
  }

  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    if (req == null) {
      val data = Map("message" -> message)
      new ServiceResponse("","",data,FSMStatus.FAILURE)	
      
    } else {
      val data = Map("uid" -> req.data("uid"), "message" -> message)
      new ServiceResponse(req.service,req.task,data,FSMStatus.FAILURE)	
    
    }
    
  }
}
