/*
 * Copyright (c) 2013-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.collectors.scalastream
package utils

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
//import java.time.Instant
//
//import cats.syntax.either._
//import com.snowplowanalytics.iglu.core._
//import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
//import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload
import io.circe.Json
import org.json4s.jackson.Serialization.{write}
import org.json4s.DefaultFormats
//import io.circe.parser._
//import io.circe.syntax._
//import org.apache.thrift.TSerializer
import model._

import scala.collection.JavaConverters._
import org.apache.commons.codec.binary.Base64
import java.net.URLDecoder

import org.joda.time.format.DateTimeFormat

import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.{SendMessageRequest}

/** Object handling splitting an array of strings correctly */
object SplitBatch {
  implicit val format = DefaultFormats

  // Serialize Thrift CollectorPayload objects
//  val ThriftSerializer = new ThreadLocal[TSerializer] {
//    override def initialValue = new TSerializer()
//  }

  /**
   * Split a list of strings into batches, none of them exceeding a given size
   * Input strings exceeding the given size end up in the failedBigEvents field of the result
   * @param input List of strings
   * @param maximum No good batch can exceed this size
   * @param joinSize Constant to add to the size of the string representing the additional comma
   *                 needed to join separate event JSONs in a single array
   * @return split batch containing list of good batches and list of events that were too big
   */
  def split(input: List[Json], maximum: Int, joinSize: Int = 1): SplitBatchResult = {
    @scala.annotation.tailrec
    def iterbatch(
      l: List[Json],
      currentBatch: List[Json],
      currentTotal: Long,
      acc: List[List[Json]],
      failedBigEvents: List[Json]
    ): SplitBatchResult = l match {
      case Nil => currentBatch match {
        case Nil => SplitBatchResult(acc, failedBigEvents)
        case nonemptyBatch => SplitBatchResult(nonemptyBatch :: acc, failedBigEvents)
      }
      case h :: t =>
        val headSize = getSize(h.noSpaces)
        if (headSize + joinSize > maximum) {
          iterbatch(t, currentBatch, currentTotal, acc, h :: failedBigEvents)
        } else if (headSize + currentTotal + joinSize > maximum) {
          iterbatch(l, Nil, 0, currentBatch :: acc, failedBigEvents)
        } else {
          iterbatch(t, h :: currentBatch, headSize + currentTotal + joinSize, acc, failedBigEvents)
        }
    }

    iterbatch(input, Nil, 0, Nil, Nil)
  }

  /**
   * If the CollectorPayload is too big to fit in a single record, attempt to split it into
   * multiple records.
   * @param event Incoming CollectorPayload
   * @return a List of Good and Bad events
   */
  def splitAndSerializePayload(event: CollectorPayload, maxBytes: Int): EventSerializeResult = {

    def parseUrlParameters(url: String) = {
      url.split("&").map( v => {
        val m =  v.split("=", 2).map(s => URLDecoder.decode(s, "UTF-8"))
        m(0) -> m(1)
      }).toMap
    }

//    val serializer = ThriftSerializer.get()
    println("event")
    println(event)

    val base64data = parseUrlParameters(event.querystring).getOrElse("ue_px", "")
    val data_json = play.api.libs.json.Json.parse(new String(Base64.decodeBase64(base64data)))

    println(data_json)

    val eventId = (data_json \ "data" \ "data" \ "id").get.toString
    val eventType = (data_json \ "data" \ "data" \ "type").get.toString
    val eventKey = (data_json \ "data" \ "data" \ "key").get.toString
    val eventValue = (data_json \ "data" \ "data" \ "value").get.toString
    val eventClient = (data_json \ "data" \ "data" \ "client").get.toString
    val candidateId = (data_json \ "data" \ "data" \ "candidateId").isEmpty match {
      case true => {""}
      case false => {(data_json \ "data" \ "data" \ "candidateId").get.toString}
    }
    val jobId = (data_json \ "data" \ "data" \ "jobId").isEmpty match {
      case true => {""}
      case false => {(data_json \ "data" \ "data" \ "jobId").get.toString}
    }
    val eventDate = DateTimeFormat.forPattern("yyyy-MM-dd").print(event.timestamp)

    println(eventId)
    println(eventType)
    println(eventKey)
    println(eventValue)
    println(eventClient)
    println(eventDate)
    println(candidateId)
    println(jobId)

    val new_event = JSONCollectorPayload(
      event.ipAddress,
      event.timestamp,
      event.encoding,
      event.collector,
      event.userAgent,
      event.refererUri,
      event.path,
      event.querystring,
      event.body,
      event.headers.asScala.toList,
      event.contentType,
      event.hostname,
      event.networkUserId,
      eventId,
      eventType,
      eventKey,
      eventValue,
      eventClient,
      eventDate,
      candidateId,
      jobId
    )

    val new_event_json = JSONCollectorPayload.write(new_event)

    println(new_event_json)
    val SQS: AmazonSQS = AmazonSQSClientBuilder.standard.withRegion(Regions.US_EAST_1).build
    val sendMessageRequest = new SendMessageRequest("https://sqs.us-east-1.amazonaws.com/997116068644/cdp-staging-snowplow-tracker", write(new_event))
    SQS.sendMessage(sendMessageRequest)

    val everythingSerialized = (new_event_json.toString + "\n").getBytes()
    val wholeEventBytes = getSize(everythingSerialized)

    // If the event is below the size limit, no splitting is necessary
    if (wholeEventBytes < maxBytes) {
      EventSerializeResult(List(everythingSerialized), Nil)
    } else {
      EventSerializeResult(List("".getBytes()), Nil)
    }
//    else {
//      (for {
//        body <- Option(event.getBody).toRight("GET requests cannot be split")
//        children <- splitBody(body)
//        initialBodyDataBytes = getSize(Json.arr(children._2: _*).noSpaces)
//        _ <- Either.cond[String, Unit](
//          wholeEventBytes - initialBodyDataBytes < maxBytes,
//          (),
//          "cannot split this POST request because event without \"data\" field is still too big"
//        )
//        splitted = split(children._2, maxBytes - wholeEventBytes + initialBodyDataBytes)
//        goodSerialized = serializeBatch(serializer, event, splitted.goodBatches, children._1)
//        badList = splitted.failedBigEvents.map { e =>
//          val msg = "this POST request split is still too large"
//          oversizedPayload(event, getSize(e), maxBytes, msg)
//        }
//      } yield EventSerializeResult(goodSerialized, badList)).fold({
//        msg =>
//          val tooBigPayload = oversizedPayload(event, wholeEventBytes, maxBytes, msg)
//          EventSerializeResult(Nil, List(tooBigPayload))
//        },
//        identity
//      )
//    }
  }

//  def splitBody(body: String): Either[String, (SchemaKey, List[Json])] = for {
//    json <- parse(body)
//      .leftMap(e => s"cannot split POST requests which are not json ${e.getMessage}")
//    sdd <- json.as[SelfDescribingData[Json]]
//      .leftMap(e => s"cannot split POST requests which are not self-describing ${e.getMessage}")
//    array <- sdd.data.asArray
//      .toRight("cannot split POST requests which do not contain a data array")
//  } yield (sdd.schema, array.toList)

//  /**
//   * Creates a bad row while maintaining a truncation of the original payload to ease debugging.
//   * Keeps a tenth of the original payload.
//   * @param event original payload
//   * @param size size of the oversized payload
//   * @param maxSize maximum size allowed
//   * @param msg error message
//   * @return the created bad rows as json
//   */
//  private def oversizedPayload(
//    event: CollectorPayload,
//    size: Int,
//    maxSize: Int,
//    msg: String
//  ): Array[Byte] =
//    BadRow.SizeViolation(
//      Processor(generated.BuildInfo.name, generated.BuildInfo.version),
//      Failure.SizeViolation(Instant.now(), maxSize, size, s"oversized collector payload: $msg"),
//      Payload.RawPayload(event.toString().take(maxSize / 10))
//    ).asJson.noSpaces.getBytes(UTF_8)

  private def getSize(a: Array[Byte]): Int = ByteBuffer.wrap(a).capacity

  private def getSize(s: String): Int = getSize(s.getBytes(UTF_8))

//  private def getSize(j: Json): Int = getSize(j.noSpaces)

//  private def serializeBatch(
//    serializer: TSerializer,
//    event: CollectorPayload,
//    batches: List[List[Json]],
//    schema: SchemaKey
//  ): List[Array[Byte]] =
//    batches.map { batch =>
//      val payload = event.deepCopy()
//      val body = SelfDescribingData[Json](schema, Json.arr(batch: _*))
//      payload.setBody(body.asJson.noSpaces)
//      serializer.serialize(payload)
//    }
}
