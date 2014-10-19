package de.kp.spark.fsm.sink
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

import java.util.Date

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.redis.RedisClient

import scala.collection.JavaConversions._

class RedisSink {

  val client  = RedisClient()
  val service = "fsm"
  
  def addPatterns(req:ServiceRequest, patterns:FSMPatterns) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "pattern:" + service + ":" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializePatterns(patterns)
    
    client.zadd(k,timestamp,v)
    
  }

  def addRules(req:ServiceRequest, rules:FSMRules) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "rule:" + service + ":" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializeRules(rules)
    
    client.zadd(k,timestamp,v)
    
  }
 
  def patternsExist(uid:String):Boolean = {

    val k = "pattern:" + service + ":" + uid
    client.exists(k)
    
  }
   
  def rulesExist(uid:String):Boolean = {

    val k = "rule:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def patterns(uid:String):String = {

    val k = "pattern:" + service + ":" + uid
    val patterns = client.zrange(k, 0, -1)

    if (patterns.size() == 0) {
      Serializer.serializePatterns(new FSMPatterns(List.empty[FSMPattern]))
    
    } else {
      
      val last = patterns.toList.last
      last.split(":")(1)
      
    }
  
  }
  
  def rules(uid:String):String = {

    val k = "rule:" + service + ":" + uid
    val rules = client.zrange(k, 0, -1)

    if (rules.size() == 0) {
      Serializer.serializeRules(new FSMRules(List.empty[FSMRule]))
    
    } else {
      
      val last = rules.toList.last
      last.split(":")(1)
      
    }
  
  }

  /**
   * Retrieve those rules, where the antecedents match
   * the provided ones
   */
  def rulesByAntecedent(uid:String, antecedent:List[Int]):String = {
  
    /* Restrict to those rules, that match the antecedents */
    val items = rulesAsList(uid).filter(rule => isEqual(rule.antecedent,antecedent))
    Serializer.serializeRules(new FSMRules(items))
    
  } 
  /**
   * Retrieve those rules, where the consequents match
   * the provided ones
   */
  def rulesByConsequent(uid:String, consequent:List[Int]):String = {
  
    /* Restrict to those rules, that match the consequents */
    val items = rulesAsList(uid).filter(rule => isEqual(rule.consequent,consequent))
    Serializer.serializeRules(new FSMRules(items))

  } 
  
  private def rulesAsList(uid:String):List[FSMRule] = {

    val k = "rule:" + service + ":" + uid
    val rules = client.zrange(k, 0, -1)

    if (rules.size() == 0) {
      List.empty[FSMRule]
    
    } else {
      
      val last = rules.toList.last
      Serializer.deserializeRules(last.split(":")(1)).items
      
    }
  
  }
  
  private def isEqual(itemset1:List[Int],itemset2:List[Int]):Boolean = {
    
    if (itemset1.length != itemset2.length) {
      return false
    }
    var sum:Int = 0
    (0 until itemset1.length).foreach(i => {
      sum += Math.abs(itemset1(i) - itemset2(i))
    })
    
    (sum == 0)
    
  }

}