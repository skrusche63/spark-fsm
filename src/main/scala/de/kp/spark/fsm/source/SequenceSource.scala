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

import de.kp.spark.core.source.{ElasticSource,FileSource,JdbcSource}

import de.kp.spark.fsm.Configuration
import de.kp.spark.fsm.model.Sources

import de.kp.spark.fsm.spec.Fields

/**
 * A SequenceSource is an abstraction layer on top of
 * different physical data source to retrieve a sequence
 * database compatible with the SPADE and TSR algorithm
 */
class SequenceSource (@transient sc:SparkContext) {

  private val model = new SequenceModel(sc)
  
  def get(data:Map[String,String]):RDD[(Int,String)] = {
    
    val uid = data("uid")
    
    val source = data("source")
    source match {
      
      /* 
       * Retrieve sequence database persisted as an appropriate 
       * search index from Elasticsearch; the configuration
       * parameters are retrieved from the service configuration 
       */    
      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(sc).connect(data)
        model.buildElastic(uid,rawset)
        
      }
      /* 
       * Retrieve sequence database persisted as a file on the (HDFS) 
       * file system; the configuration parameters are retrieved from 
       * the service configuration  
       */    
      case Sources.FILE => {

        val path = Configuration.file()
        
        val rawset = new FileSource(sc).connect(data,path)
        model.buildFile(uid,rawset)
        
      }
      /*
       * Retrieve sequence database persisted as an appropriate table 
       * from a JDBC database; the configuration parameters are retrieved 
       * from the service configuration
       */
      case Sources.JDBC => {
    
        val fields = Fields.get(uid).map(kv => kv._2._1).toList    
        
        val rawset = new JdbcSource(sc).connect(data,fields)
        model.buildJDBC(uid,rawset)
        
      }
      /*
       * Retrieve sequence database persisted as an appropriate table from 
       * a Piwik database; the configuration parameters are retrieved from 
       * the service configuration
       */
      case Sources.PIWIK => {
        
        val rawset = new PiwikSource(sc).connect(data)
        model.buildPiwik(uid,rawset)
        
      }
            
      case _ => null
      
    }

  }
  
}