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
import akka.routing.RoundRobinRouter

import de.kp.spark.fsm.Configuration
import de.kp.spark.fsm.model._

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

class FSMMaster extends Actor with ActorLogging {
  
  /* Load configuration for routers */
  val (time,retries,workers) = Configuration.router   

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(time).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }

  val miner = context.actorOf(Props[FSMMiner])
  val questor = context.actorOf(Props[FSMQuestor].withRouter(RoundRobinRouter(workers)))
  
  def receive = {
    
    case req:String => {
      
      implicit val ec = context.dispatcher

      val duration = Configuration.actor      
      implicit val timeout:Timeout = DurationInt(duration).second
	  	    
	  val origin = sender

	  val deser = FSMModel.deserializeRequest(req)
	  val response = deser.task match {
        
        case "train"  => ask(miner,deser).mapTo[ServiceResponse]
        case "status" => ask(miner,deser).mapTo[ServiceResponse]
        
        case "rules" => ask(questor,deser).mapTo[ServiceResponse]
        case "patterns" => ask(questor,deser).mapTo[ServiceResponse]

        case "predict" => ask(questor,deser).mapTo[ServiceResponse]
       
        case _ => {

          Future {          
            failure(deser,Messages.TASK_IS_UNKNOWN(deser.data("uid"),deser.task))
          } 
          
        }
      
      }
      response.onSuccess {
        case result => origin ! FSMModel.serializeResponse(result)
      }
      response.onFailure {
        case result => origin ! FSMStatus.FAILURE	      
	  }
      
    }
  
    case _ => {}
    
  }

  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    val data = Map("uid" -> req.data("uid"), "message" -> message)
    new ServiceResponse(req.service,req.task,data,FSMStatus.FAILURE)	
    
  }
}
