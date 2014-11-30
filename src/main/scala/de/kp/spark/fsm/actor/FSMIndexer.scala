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

import de.kp.spark.core.model._
import de.kp.spark.core.io.ElasticIndexer

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.io.{ElasticBuilderFactory => EBF}

class FSMIndexer extends BaseActor {
  
  def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data("uid")

      try {

        val index   = req.data("index")
        val mapping = req.data("type")
    
        val topic = req.task match {
          
          case "index:item" => "item"
          
          case "index:rule" => "rule"
          
          case _ => {
            
            val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
            throw new Exception(msg)
            
          }
        
        }
        
        val builder = EBF.getBuilder(topic,mapping)
        val indexer = new ElasticIndexer()
    
        indexer.create(index,mapping,builder)
        indexer.close()
      
        val data = Map("uid" -> uid, "message" -> Messages.SEARCH_INDEX_CREATED(uid))
        val response = new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)	
      
        val origin = sender
        origin ! response
      
      } catch {
        
        case e:Exception => {
          
          log.error(e, e.getMessage())
      
          val data = Map("uid" -> uid, "message" -> e.getMessage())
          val response = new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
      
          val origin = sender
          origin ! response
          
        }
      
      } finally {
        
        context.stop(self)

      }
    }
    
  }
  
}