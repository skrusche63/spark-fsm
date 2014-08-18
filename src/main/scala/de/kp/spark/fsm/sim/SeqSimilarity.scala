package de.kp.spark.fsm.sim
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

import java.util.Random
import scala.collection.mutable.ArrayBuffer

/**
 * This is a Scala implementation of the S2MP algorithm
 * to measure the similarity of two sequences of itemsets
 * with variable length
 * 
 * http://crpit.com/confpapers/CRPITV87Saneifar.pdf
 */
class SeqSimilarity(mapcoeff:Double=0.5, ordcoeff:Double=0.5) {
 
  /*
   * The mapping score measure the resemblance of 
   * two sequences based on the links that can be
   * established between them
   */
  private var mapScore:Double = 0.0
 
  /*
   * The order score measures the resemblance of
   * the two sequences based on the order and positions
   * of the itemsets in the sequences
   */
  private var ordScore:Double = 0.0
  
  private var len1:Int = 0
  private var seq1:Array[Array[Int]] = null
  
  private var len2:Int = 0
  private var seq2:Array[Array[Int]] = null
  
  private var mapOrder:Array[Int] = null
  private var weights:Array[Array[Double]] = null
  
  /** 
   * This method calculates the similarity 
   * between two sequences of Array[Int] 
   */
  def compute(seq1:Array[Array[Int]], seq2:Array[Array[Int]], method:String="cosine"):Double = {

    this.seq1 = seq1
    this.len1 = seq1.length

    this.seq2 = seq2
    this.len2 = seq2.length

    /**
     * Initialize data structures
     */
    mapScore = 0.0
    ordScore = 0.0
    
    mapOrder = Array.fill[Int](len1)(0)    
    weights = calcWeights(method)
        
    calcMapScore()
    calcOrdScore()

    (mapcoeff * this.mapScore) + (ordcoeff * this.ordScore)
  
  }
  
  /** 
   * Calculate the matching weights between each pair 
   * of itemsets in the sequences and store as matrix 
   */
  private def calcWeights(method:String):Array[Array[Double]] = {

    val matrix = Array.fill[Double](len1,len2)(0)
    /* 
     * For each ith itemset in seq1, calculate 
     * matching weight for each jth item in seq2 
     */
    (0 until len1).foreach(i => {
      (0 until len2).foreach(j => {
                    
        matrix(i)(j) = method match {
          
          case "cosine"       => cosineSimilarity(seq1(i),seq2(j))          
          case "intersection" => intersectionSimilarity(seq1(i),seq2(j))
          case "jacquard"     => jaccardSimilarity(seq1(i),seq2(j))
          
          case _ => 0.0
          
        }
      
      })
    
    })

    matrix
    
  }

  /**
   * Cosine similarity for two itemsets with variable length; 
   * this method is more sensitive to the itemset content than
   * intersection similarity (which has been proprosed by the 
   * S2MP algorithm)
   */
  private def cosineSimilarity(itemset1:Array[Int],itemset2:Array[Int]):Double = {
    
    val ln1 = itemset1.length
    val ln2 = itemset2.length
    
    if (ln1 == ln2) {
      
      CosineSimilarity.compute(itemset1,itemset2)
      
    } else if (ln1 < ln2) {
      
      val tmp = Array.fill(ln2)(0)
      (0 until ln1).foreach(i => tmp(i) = itemset1(i))
      
      CosineSimilarity.compute(tmp,itemset2)
      
    } else {
      
      val tmp = Array.fill(ln1)(0)
      (0 until ln2).foreach(i => tmp(i) = itemset2(i))
      
      CosineSimilarity.compute(itemset1,tmp)
     
    }
    
  }
  
  private def intersectionSimilarity(itemset1:Array[Int],itemset2:Array[Int]):Double = {
      
    val list1 = itemset1.toList
    val size1 = list1.size
      
    val list2 = itemset2.toList
    val size2 = list2.size
        
    /*
     * Intersection means that (1,2,3) is equal to (1,3,2)
     */
    val intersection = list1.intersect(list2)
    intersection.size.toDouble / ((size1+size2).toDouble / 2.0)
    
  }

  private def jaccardSimilarity(itemset1:Array[Int],itemset2:Array[Int]):Double = {

    val list1 = itemset1.toList
    val list2 = itemset2.toList

    val intersection = list1.intersect(list2)
    val union = list1.union(list2)
    
    intersection.size.toDouble / union.size

  }
  
  private def calcMapScore() {

    /* 
     * Keep track of the number of matching weights equal to zero 
     */
    var count_minus_ones = 0

    /* 
     * Find the position of each row's max matching weight; 
     * note, that a row represents the weights of a certain 
     * itemset in seq1 for all itemsets in seq2
     */
    (0 until this.weights.size).foreach(i => {

      val weights_i = this weights(i)
      /* 
       * In the case of two matching max weights, 
       * we take the first one 
       */
      var max_pos = weights_i.indexOf(weights_i.max)

      /* 
       * Set all mappings associated with a meanininglessly 
       * small weight to -1 
       */
      if (weights(i)(max_pos) < 0.00001) {
        
        max_pos = -1
        count_minus_ones += 1
      
      }
      
      /* 
       * Mapping order holds the maximum weight for itemset i from seq1 
       * and all other itemsets from seq2, or in other words, the position
       * of the itemset from seq2 with the highest similarity
       */
      this.mapOrder(i) = max_pos
     
    })
        
    /* 
     * If there are no mappings, return with mapping score = 0 
     */    
    if (count_minus_ones == this.mapOrder.size) {
      this.mapScore = 0.0
      return
    }
    
    /* 
     * Now detect conflicts/duplicates (two itemsets in seq1 mapped to the same itemset in seq2) 
     */
    val mo_size = this.mapOrder.size
    (0 until mo_size).foreach(i => {

      val mo_i = this mapOrder(i)
      /* 
       * If we have a non zero matching weight, find conflict and resolve 
       */
      if (mo_i > -1) {
        
        ((i+1) until mo_size).foreach(k => {
          if (mo_i == this.mapOrder(k)) {
            solveConflict(i,k)
          }
        })
      } 
      
    })
        
    /* 
     * Set mapping score to the mean of the heighest weights 
     * of the mapped itemsets and return
     */    
    var count:Int = 0
    var total:Double = 0.0

    (0 until mo_size).foreach(i => {
      
      val mo_i = this mapOrder(i)
      if (mo_i > -1) {
         total += this.weights(i)(mo_i)
         count += 1
      }
    
    })
    
    this.mapScore = if (count == 0) 0.0 else total / count
  
  }

  /* 
   * The conflict: mapping order (i) and mapping order(k) are equal.
   * 
   * Need to find possible resolutions to this conflict and pick the best one.
   */
  private def solveConflict(i:Int, k:Int) {

    /* 
     * First find what itemsets in seq2 are still available to be mapped to 
     */

    /* Vector to hold indices of available seq2 itemsets */
    val list1 = (0 until len2).toList 
    
    /* 
     * Find which of these ids are NOT in mapping order (and store in avbl) 
     */    
    val list2 = this.mapOrder.toList
    
    val avbl = list1.diff(list2)

    /* 
     * Now evaluate all the scores for keeping mapping order (i) 
     * the same and changing mapping order(k) 
     */
    var no_alternative_found = true
    
    var score:Double = 0
    
    var sc1:Double = 0.0
    var sc1Id:Int = 0

    (0 until avbl.length).foreach(j => {
            
      val avbl_j = avbl(j)
      val mo_i = this.mapOrder(i)
      
      score = calculateLocalSimilarity(i, mo_i, this.weights(i)(mo_i), k, avbl_j, this.weights(k)(avbl_j))
      
      if (score > sc1) {
        sc1 = score
        sc1Id = avbl(j)
        
        no_alternative_found = false
      }
      
    })
    
    /* 
     * Now evaluate all the scores for keeping mappingorder (k) 
     * the same and changing mapping order(i) 
     */
    var sc2:Double = 0.0
    var sc2Id:Int = 0

    (0 until avbl.length).foreach(j => {
      
      val avbl_j = avbl(j)
      val mo_k = this.mapOrder(k)
      
      score = calculateLocalSimilarity(i, avbl_j, this.weights(i)(avbl_j), k, mo_k, this.weights(k)(mo_k))
            
      if (score > sc2) {
        sc2 = score
        sc2Id = avbl(j)
                
        no_alternative_found = false
      
      }
      
    })
    
    /* Pick the best resolution to the conflict */

    /* If no alternatives were found, remove the 
     * mapping associated with the lower weight 
     */
    if (no_alternative_found) {

      /* 
       * If matching weight associated with mapping order (i) is greater, then keep i 
       */      
      if (this.weights(i)(this.mapOrder(i)) > this.weights(k)(this.mapOrder(k))) {
        this.mapOrder(k) = -1
     
      } else {
        this.mapOrder(i) = -1
      
      }
    } else if (sc1 > sc2) {
      /* 
       * If the best resolution was found by changing the mapping order (k), then implement this change 
       */
      this.mapOrder(k) = sc1Id

    } else {
      /* Else the best resolution is implemented by changing mapOrder (i) */
      this.mapOrder(i) = sc2Id
    }
  
  }
   
  private def calculateLocalSimilarity(id1:Int, cid1:Int, w1:Double, id2:Int, cid2:Int, w2:Double):Double = {
        
    val cond1 = (id1 > id2) && (cid1 > cid2)
    val cond2 = (id1 < id2) && (cid1 < cid2)
    
    if (cond1 || cond2) (w1+w2) / 2.0 else (w1+w2) / 4.0
  
  }
   
  /**
   * The method calculate the order score; to this end, the positions 
   * of the highest weights are compared to the positions of the itemsets
   * in seq1: Suppose itemset1 and itemset2 are equal, and {(ab),c,(de)},
   * then the mapOrder is computed as {0,1,2}
   */
  private def calcOrdScore() {

    this.ordScore = 0.0

    var total_order:Double = 0.0
    var position_order:Double = 0.0
    
    /**
     * Hold data about mapping positions
     * that refer to non-zero weights
     */
    val subSeq    = ArrayBuffer.empty[Int]
    val subSeqIds = ArrayBuffer.empty[Int]
    
    val meanlen:Double = (len1 + len2).toDouble / 2.0
    
    var score:Double = 0.0
    
    var sm:Double = 0.0

    /* 
     * Create an augmented version of mapping order with a 0 at the end
     * (necessary for method used to detect increasing subsequences)
     */
    val temp_mo = Array.fill[Int](len1 + 1)(0)
    (0 until this.mapOrder.length).foreach(i => temp_mo(i) = mapOrder(i))

    /*
     * Evaluate temporary mapping order
     */
    (0 until (len1 + 1)).foreach(i => {
            
      if (i == 0 || subSeq.size == 0) {
                
        if (temp_mo(i) > -1) {
                    
          subSeq += temp_mo(i)
          subSeqIds += i
                
        }
      
      } else {
      
        /* 
         * If this mapping order value is less than the 
         * previous one, need to end subsequence
         */
        if (temp_mo(i) < temp_mo(i-1)) {

            /* Calculate total order */
            total_order = (subSeq.length).toDouble / meanlen

            /* Calculate position order */
            position_order = if (subSeq.length == 1) 0.0
              else {
            
                sm = 0.0
                (1 until subSeq.length).foreach(j => {
                  sm += Math.abs(subSeq(j) - subSeq(j-1)) - Math.abs(subSeqIds(j) - subSeqIds(j-1))
                })
                      
                sm.toDouble / meanlen
          
            }
                  
            /* Calculate score for this subsequence */
            score = total_order * (1.0 - position_order)

            /* If this is the best score yet, then use it */
            if (score > this.ordScore) {
              this.ordScore = score
            }
          
            /* Clear subsequence */
            subSeq.clear
            subSeqIds.clear

            if (temp_mo(i) > -1) {
              subSeq += temp_mo(i)
              subSeqIds += i
                  
            }

          } else {
        
          if (temp_mo(i) > -1) {
            subSeq += temp_mo(i)
            subSeqIds += i
          }
        
        }
      
      } 
    
    })
  
  }
                
}