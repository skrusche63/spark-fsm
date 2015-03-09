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

import de.kp.spark.core.Names
import de.kp.spark.core.actor.RootActor

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisCache

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.Configuration

abstract class BaseActor extends RootActor(Configuration) {
  
  protected def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data(Names.REQ_UID)
    
    if (missing == true) {
      val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
  
    } else {
      val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.MINING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.MINING_STARTED)	
  
    }

  }

}