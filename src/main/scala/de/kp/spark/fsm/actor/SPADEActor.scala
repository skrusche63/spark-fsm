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

import de.kp.spark.core.source.SequenceSource
import de.kp.spark.core.source.handler.SPMFHandler

import de.kp.spark.fsm.{RequestContext,SPADE}

import de.kp.spark.fsm.model._

import de.kp.spark.fsm.spec.SequenceSpec
import scala.collection.mutable.ArrayBuffer

class SPADEActor(@transient ctx:RequestContext) extends TrainActor(ctx) {
  
  override def train(req:ServiceRequest) {
          
    val source = new SequenceSource(ctx.sc,ctx.config,new SequenceSpec(req))
    val dataset = SPMFHandler.sequence2SPMF(source.connect(req))
      
    val params = ArrayBuffer.empty[Param]
    
    val support = req.data("support").toDouble        
    params += Param("support","double",support.toString)

    cache.addParams(req, params.toList)
     
    val patterns = SPADE.extractRDDPatterns(dataset,support).map(pattern => {
      
      val line = pattern.serialize()
      // 1 -1 3 -1 3 -1 | 3
      val Array(sequence,cardinality) = line.split("\\|")
      
      val support = cardinality.trim().toInt
      val itemsets = sequence.trim().split("-1").map(itemset => itemset.trim().split(" ").map(_.toInt).toList).toList

      new Pattern(support,itemsets)
      
    }).toList
          
    savePatterns(req,new Patterns(patterns))

  }  
  
  private def savePatterns(req:ServiceRequest,patterns:Patterns) {
    
    redis.addPatterns(req,patterns)
    
  }
  
  override def validate(req:ServiceRequest) {

    if (req.data.contains("name") == false) 
      throw new Exception("No name for sequential patterns provided.")

    if (req.data.contains("support") == false)
      throw new Exception("Parameter 'support' is missing.")
    
  }
  
}