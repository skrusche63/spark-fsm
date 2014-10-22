package de.kp.spark.fsm.rest
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

import org.apache.spark.SparkContext

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.pattern.ask

import akka.util.Timeout

import spray.http.StatusCodes._

import spray.routing.{Directives,HttpService,RequestContext,Route}

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.spark.fsm.Configuration

import de.kp.spark.fsm.actor.FSMMaster
import de.kp.spark.fsm.model._

class RestApi(host:String,port:Int,system:ActorSystem,@transient val sc:SparkContext) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.fsm.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  val (duration,retries,time) = Configuration.actor   
  val master = system.actorOf(Props(new FSMMaster(sc)), name="FSMMaster")

  def start() {
    RestService.start(routes,system,host,port)
  }

  /*
   * The routes defines the different access channels this API supports
   */
  private def routes:Route = {

    /*
     * Action specifies a concept that is supported by the REST service;
     * other concepts are content,feature,product and state
     */
    path("train") {
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx)
	    }
	  }
    }  ~ 
    path("get" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,subject)
	    }
	  }
    }  ~ 
    path("status") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx)
	    }
	  }
    }  ~ 
    path("track") {
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrack(ctx)
	    }
	  }
    } 
  
  }

  private def doGet[T](ctx:RequestContext,subject:String) = {
	    
    subject match {

      case "patterns" => doRequest(ctx,"series","get:pattern")
      
	  case "rules" => doRequest(ctx,"series","get:rule")
	      
	  case _ => {}
	  
    }
    
  }
  
  private def doTrack[T](ctx:RequestContext) = doRequest(ctx,"series","track")

  private def doTrain[T](ctx:RequestContext) = doRequest(ctx,"series","train")

  private def doStatus[T](ctx:RequestContext) = doRequest(ctx,"series","status")
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String="train") = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(master,request).mapTo[ServiceResponse] 
    ctx.complete(response)
    
  }

  private def getHeaders(ctx:RequestContext):Map[String,String] = {
    
    val httpRequest = ctx.request
    
    /* HTTP header to Map[String,String] */
    val httpHeaders = httpRequest.headers
    
    Map() ++ httpHeaders.map(
      header => (header.name,header.value)
    )
    
  }
 
  private def getBodyAsMap(ctx:RequestContext):Map[String,String] = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    val body = JSON.parseFull(httpEntity.data.asString) match {
      case Some(map) => map
      case None => Map.empty[String,String]
    }
      
    body.asInstanceOf[Map[String,String]]
    
  }
  /**
   * This method returns the 'raw' body provided with a Http request;
   * it is e.g. used to access the meta service to register metadata
   * specifications
   */
  private def getBodyAsString(ctx:RequestContext):String = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    httpEntity.data.asString
    
  }
  
  private def getRequest(ctx:RequestContext):Map[String,String] = {

    val headers = getHeaders(ctx)
    val body = getBodyAsMap(ctx)
    
    headers ++ body
    
  }

}