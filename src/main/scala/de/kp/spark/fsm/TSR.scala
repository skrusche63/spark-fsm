package de.kp.spark.fsm
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
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import org.apache.hadoop.io.{LongWritable,Text}
import org.apache.hadoop.mapred.TextInputFormat

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import de.kp.spark.fsm.source.FileSource
import de.kp.core.tsr.{Rule,Sequence,TSRAlgorithm,Vertical}

object TSR extends Serializable {
  
  def extractFileRules(@transient sc:SparkContext,k:Int, minconf:Double):List[Rule] = {
    
    val dataset = new FileSource(sc).connect()
    extractRDDRules(dataset,k,minconf)
    
  }
  
  def extractRDDRules(dataset:RDD[(Int,String)],k:Int, minconf:Double):List[Rule] = {
              
    val sc = dataset.context
    
    /**
     * STEP #1
     * 
     * Determine max & min item; note, that we have to make sure,
     * that ctrl integer, i.e. -1 and -2 are not taken into account
     */
    val ids = dataset.flatMap(data => data._2.split(" ").map(_.toInt).filter(v => v > -1).toList.distinct).collect()
    
    val max = sc.broadcast(ids.max)
    val min = sc.broadcast(ids.min)
    /**
     * STEP #2
     * 
     * Build sequences
     */
    val sequences = dataset.map(valu => newSequence(valu._1,valu._2))
    
    def seqOp(vert:Vertical, seq:Sequence):Vertical = {
      
      vert.setSize(max.value,min.value)
      
      val sid = seq.getId()           
      for (j <- 0 until seq.getItemsets().size()) {
              
        val itemset = seq.get(j)
        for (i <- 0 until itemset.length) {
                
          val itemI = itemset(i)  
		  if (vert.arrayMapItemCountFirst(itemI) == null) {
			
		    vert.arrayMapItemCountFirst(itemI) = new java.util.HashMap[Integer,Integer]()
			vert.arrayMapItemCountLast(itemI)  = new java.util.HashMap[Integer,Integer]()
					
		  }
          
          val oldpos = vert.arrayMapItemCountFirst(itemI).get(sid)
                
          if (oldpos == null) {
            vert.arrayMapItemCountFirst(itemI).put(sid, j)
			vert.arrayMapItemCountLast(itemI).put(sid, j)
                
          } else{
		    vert.arrayMapItemCountLast(itemI).put(sid, j)
                
          }

        }
        
      }
            
      vert
      
    }
    
    /*
     * Note that vert1 is always NULL
     */
    def combOp(vert1:Vertical,vert2:Vertical):Vertical = vert2      

    val vertical = sequences.coalesce(1, false).aggregate(new Vertical())(seqOp,combOp)    
    val seqs = sequences.collect()
    
    /**
     * STEP #3
     * 
     * Apply algorithm to determine frequent sequence patterns
     */ 
    val algorithm = new TSRAlgorithm()
    val rules = algorithm.runAlgorithm(vertical,seqs, k,minconf)

    rules.toList
    
  }
  
  private def newSequence(sid:Int,seq:String):Sequence = {
          
    val items = seq.split(" ")
    val sequence = new Sequence(sid)
      
    var itemset = ArrayBuffer.empty[java.lang.Integer]
    for (item <- items) {
  	  /*
	   * If the token is -1, it means that we reached the end of an itemset.
	   */
      if (item == "-1") { 
		/*
		 * Add the current itemset to the sequence
	     */
        sequence.addItemset(itemset.toArray)
	    itemset = ArrayBuffer.empty[java.lang.Integer]
		
      }
	  /*
	   * If the token is -2, it means that we reached the end of 
	   * the sequence
	   */
	  else if (item == "-2") { 
	    /* do nothing */
	  }
      else { 
		itemset += Integer.parseInt(item)
        
      }        
      
    }

    sequence
    
  }
  
}