package de.kp.spark.fsm.redis
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

import de.kp.spark.fsm.model._

import java.util.Date
import scala.collection.JavaConversions._

object RedisCache {

  val client  = RedisClient()
  val service = "series"

  def addFields(req:ServiceRequest,fields:Fields) {
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "fields:" + service + ":" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializeFields(fields)
    
    client.zadd(k,timestamp,v)
    
  }
  
  def addRequest(req:ServiceRequest) {
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "request:" + service
    val v = "" + timestamp + ":" + Serializer.serializeRequest(req)
    
    client.lpush(k,v)
    
  }
  
  def addStatus(req:ServiceRequest, status:String) {
   
    val (uid,task) = (req.data("uid"),req.task)
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "job:" + service + ":" + uid
    val v = "" + timestamp + ":" + Serializer.serializeJob(JobDesc(service,task,status))
    
    client.zadd(k,timestamp,v)
    
  }
  
  def fieldsExist(uid:String):Boolean = {

    val k = "fields:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def taskExists(uid:String):Boolean = {

    val k = "job:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def fields(uid:String):Fields = {

    val k = "fields:" + service + ":" + uid
    val metas = client.zrange(k, 0, -1)

    if (metas.size() == 0) {
      new Fields(List.empty[Field])
    
    } else {
      
      val latest = metas.toList.last
      val Array(timestamp,fields) = latest.split(":")
      
      Serializer.deserializeFields(fields)
     
    }

  }
  
  def requestsTotal():Long = {

    val k = "request:" + service
    if (client.exists(k)) client.llen(k) else 0
    
  }
 
  def requests(start:Long,end:Long):List[(Long,ServiceRequest)] = {
    
    val k = "request:" + service
    val requests = client.lrange(k, start, end)
    
    requests.map(request => {
      
      val Array(ts,req) = request.split(":")
      (ts.toLong,Serializer.deserializeRequest(req))
      
    }).toList
    
  }
  
  def status(uid:String):String = {

    val k = "job:" + service + ":" + uid
    val data = client.zrange(k, 0, -1)

    if (data.size() == 0) {
      null
    
    } else {
      
      /* Format: timestamp:jobdesc */
      val last = data.toList.last
      val Array(timestamp,jobdesc) = last.split(":")
      
      val job = Serializer.deserializeJob(jobdesc)
      job.status
      
    }

  }
  
  def statuses(uid:String):List[(Long,JobDesc)] = {
    
    val k = "job:" + service + ":" + uid
    val data = client.zrange(k, 0, -1)

    if (data.size() == 0) {
      null
    
    } else {
      
      data.map(record => {
        
        val Array(timestamp,jobdesc) = record.split(":")
        (timestamp.toLong,Serializer.deserializeJob(jobdesc))
        
      }).toList
      
    }
    
  }
}