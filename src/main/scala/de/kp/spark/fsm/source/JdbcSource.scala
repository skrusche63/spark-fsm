package de.kp.spark.fsm.source
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

import de.kp.spark.fsm.Configuration
import de.kp.spark.fsm.io.JdbcReader

import de.kp.spark.fsm.spec.Fields

class JdbcSource(@transient sc:SparkContext) extends Source(sc) {
  
  override def connect(params:Map[String,Any]):RDD[(Int,String)] = {
    
    val uid = params("uid").asInstanceOf[String]    
    
    val fieldspec = Fields.get(uid)
    val fields = fieldspec.map(kv => kv._2._1).toList    
    /*
     * Convert field specification into broadcast variable
     */
    val spec = sc.broadcast(fieldspec)
    
    /* Retrieve site and query from params */
    val site = params("site").asInstanceOf[Int]
    val query = params("query").asInstanceOf[String]

    val rawset = new JdbcReader(sc,site,query).read(fields)
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1).asInstanceOf[String]
      val timestamp = data(spec.value("timestamp")._1).asInstanceOf[Long]

      val user = data(spec.value("user")._1).asInstanceOf[String] 
      val group = data(spec.value("group")._1).asInstanceOf[String]
      
      val item  = data(spec.value("item")._1).asInstanceOf[Int]
      
      (site,user,group,timestamp,item)
      
    })
    
    /*
     * Group dataset by site & user and aggregate all items of a
     * certain group and all groups into a time-ordered sequence
     * representation that is compatible to the SPMF format.
     */
    val sequences = dataset.groupBy(v => (v._1,v._2)).map(data => {
      
      /*
       * Aggregate all items of a certain group onto a single
       * line thereby sorting these items in ascending order.
       * 
       * And then, sort these items by timestamp in ascending
       * order.
       */
      val groups = data._2.groupBy(_._3).map(group => {

        val timestamp = group._2.head._4
        val items = group._2.map(_._5.toInt).toList.distinct.sorted.mkString(" ")

        (timestamp,items)
        
      }).toList.sortBy(_._1)
      
      /*
       * Finally aggregate all sorted item groups (or sets) in a single
       * line and use SPMF format
       */
      groups.map(_._2).mkString(" -1 ") + " -2"
      
    }).coalesce(1)

    val ids = sc.parallelize(Range.Long(0,sequences.count,1),sequences.partitions.size)
    sequences.zip(ids).map(valu => (valu._2.toInt,valu._1)).cache()

  }

}