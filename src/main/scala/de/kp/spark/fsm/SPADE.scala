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
import org.apache.spark.rdd.RDD

import de.kp.core.spade._

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer,HashMap}

import scala.util.control.Breaks._

object SPADE extends Serializable {

  private val keep = true
  private val verbose = false

  def extractRDDPatterns(dataset:RDD[(Int,String)], support:Double, dfs:Boolean=true,stats:Boolean=true):List[Pattern] = {
	
    val sc = dataset.context
    
    /**
     * STEP #1
     * 
     * Build sequences
     */
    val sequences = dataset.map(valu => newSequence(valu._1,valu._2))    
    val total = sequences.count()
    
    /**
     * STEP #2
     * 
     * Aggregate sequences into Item -> EquivalenceClass map
     */ 
    def seqOp(map:ItemEQC, seq:Sequence):ItemEQC = {
      
      val sid = seq.getId()
      
      val itemsets = seq.getItemsets()
       for (i <- 0 until itemsets.size()) yield {
         
         val itemset = itemsets.get(i);
         val timestamp = itemset.getTimestamp()
         
         for (j <- 0 until itemset.size()) yield {
           
           val item = itemset.get(j).asInstanceOf[Item[java.lang.Integer]]
           map.get(item) match {
             
             case None => {
               /**
                * Create new equivalence class
                * and register sequence
                */
               val idList = new IDListBitmap()
               idList.registerBit(sid,timestamp.toInt)
            
               val pair:ItemAbstractionPair = new ItemAbstractionPair(item, AbstractionQualitative.create(false))
            
               val pattern = new Pattern(List(pair))
               val eqc = new EquivalenceClass(pattern, idList)
               
               map += item -> eqc
                
             }
             
             case Some(eqc) => {
               /**
                * Register sequence
                */
               val idList = eqc.getIdList().asInstanceOf[IDListBitmap]
               idList.registerBit(sid,timestamp.toInt)

             }
           
           }
         
         }
       
       }
      
      map

    }
    
    def combOp(ds1:ItemEQC,ds2:ItemEQC):ItemEQC = ds2
    
    val eqcDS = sequences.coalesce(1, false).aggregate(new ItemEQC())(seqOp,combOp)
    /**
     * STEP #3
     * 
     * Reduce to equivalence classes that appear in sequences
     * above the calculated threshold
     */ 
    val minsupp = Math.ceil(support * total)      
    val freqEQC = eqcDS.values.map(eqc => {
      
      val idList = eqc.getIdList()
      
      if (idList.getSupport() >= minsupp) {
      
        idList.setAppearingSequences(eqc.getClassIdentifier())          
        eqc
          
      } else 
        null
      
    }).filter(eqc => (eqc != null)).toList
    /**
     * STEP #4
     * 
     * Apply algorithm to determine frequent sequence patterns
     */ 
    val algorithm = new SpadeAlgorithm(support, dfs)
    algorithm.runAlgorithm(freqEQC, total.toInt, keep, verbose)
        
    val patterns = algorithm.getResult()   
    if (stats) println(algorithm.printStatistics())

    patterns.toList
    
  }
  
  /**
   * A private helper method to retrieve the sequence representation from the textual SPMF format
   */
  private def newSequence(sid:Int,seq:String):Sequence = {

    /*
     * 'seq' is an SPMF compliant String representation of a sequence, e.g.
     * 1 2 -1 3 -1 4 -2 and must be split into an Array for further evaluation
     */
    val ids = seq.split(" ")
    
    var timestamp:Long = -1

    var itemset:Itemset = new Itemset()
    val sequence:Sequence = new Sequence(sid)
        
    for (i <- 0 until ids.length) yield {
          
      val id = ids(i)
      if (id.codePointAt(0) == '<') {

        /**
         * Process timestamp
         */
        val value = id.substring(1, id.length - 1)
                
        timestamp = value.toLong
        itemset.setTimestamp(timestamp)
            
      } else if (id.equals("-1")) {
        /**
         * Process end of itemset: add itemset to 
         * sequence and initialize itemset for next
         */
        val time = itemset.getTimestamp() + 1
        sequence.addItemset(itemset)
            
        itemset = new Itemset()
        itemset.setTimestamp(time)
            
            
        timestamp += 1
            
      } else if (id.equals("-2")) {
        /** 
         *  End of sequence, do nothing
         */

      } else {
        /**
         * Process item
         */
        val itemid:java.lang.Integer = id.toInt
        
        val item = new Item(itemid)
        itemset.addItem(item)
            
        if (timestamp < 0) {
              
          timestamp = 1
          itemset.setTimestamp(timestamp)
                
        }
          
      }
        
    }

    sequence
    
  }

}

class ItemEQC extends Serializable {
  
  private val keys = ArrayBuffer.empty[Item[java.lang.Integer]]
  private val vals = ArrayBuffer.empty[EquivalenceClass]
  
  def get(key:Item[java.lang.Integer]):Option[EquivalenceClass] = {
    
    var eqc:EquivalenceClass = null
    
    breakable {
      for (i <- 0 until keys.size) {
        
        if (key == keys(i)) {
          eqc = vals(i)
          break
        }
      }
      
    }
    
    (if (eqc == null) None else Some(eqc))
    
  }
  
  def += (kv: (Item[java.lang.Integer], EquivalenceClass)) {
  
    keys += kv._1
    vals += kv._2
    
  }
  
  def values = vals

}