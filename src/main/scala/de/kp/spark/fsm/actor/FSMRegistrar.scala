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
import akka.actor.{Actor,ActorLogging}

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.redis.RedisCache

import scala.collection.mutable.ArrayBuffer

class FSMRegistrar extends Actor with ActorLogging {
  
  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")
      
      val response = try {
        
        /* Unpack fields from request and register in Redis instance */
        val fields = ArrayBuffer.empty[Field]

        fields += new Field("site","string",req.data("site"))
        fields += new Field("timestamp","long",req.data("timestamp"))

        fields += new Field("user","string",req.data("user"))
        fields += new Field("group","string",req.data("group"))

        fields += new Field("item","integer",req.data("integer"))
        RedisCache.addFields(req, new Fields(fields.toList))
        
        new ServiceResponse("series","meta",Map("uid"-> uid),FSMStatus.SUCCESS)
        
      } catch {
        case throwable:Throwable => failure(req,throwable.getMessage)
      }
      
      origin ! Serializer.serializeResponse(response)

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