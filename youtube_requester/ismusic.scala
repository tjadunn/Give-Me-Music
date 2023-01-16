// Package to recognise music from the Structured streaming API
// Sends requests to yt.lemnoslife to ascertain musical information from videos

package recognisemusic

import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, HttpEntity}
import akka.http.scaladsl.Http

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import scala.concurrent.Future
import scala.concurrent.duration._

/*
 * Likkle Monad refresher
 *
 * Option defines flatmap & unit(identity)  so it's a generic monad
 *
 * class Person(var name: String, var child: Option[Person]):
 *  override def toString(): String = name
 *
 * val getchild = (person: Person) => person.child
 *
 * // this could be a call to a db for example but just use mum here for now..
 * def load(): Option[Person] =
 *  Option[noreen]
 *
 *  // get all children and grandchildren
 * load().flatMap(getchild).flatMap(getchild)
 *
 * Basically Monad(x).flatMap(f) == f(x)
*/

object YoutubeHTTPRequester {

  // akka boilerplate
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  // hard code this Skillibeng tune for now
  val request = HttpRequest(
    method = HttpMethods.GET,
    uri = "https://yt.lemnoslife.com:443/videos?part=musics&id=9W8B6uD7RnI",
  )

  def sendRequest(): Future[String] = {
    val responseFuture: Future[HttpResponse] = Http().singleRequest(request)
    // Future is a monad so we can flatMap !
    val entityFuture: Future[HttpEntity.Strict] = responseFuture.flatMap(
      _.entity.toStrict(2.seconds)
    )
    entityFuture.map(_.data.utf8String)
  }

  def main(args: Array[String]): Unit = {
    sendRequest().foreach(println)
  }
}

