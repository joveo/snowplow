package com.snowplowanalytics.snowplow.collectors.scalastream.utils

import play.api.libs.json.{JsValue, Json}

case class JSONCollectorPayload(ipAddress: String,
                                timestamp: Long,
                                encoding: String,
                                collector: String,
                                userAgent: String,
                                refererUri: String,
                                path: String,
                                querystring: String,
                                body: String,
                                headers: List[String],
                                contentType: String,
                                hostname: String,
                                networkUserId: String,
                                eventId: String,
                                eventType: String,
                                eventKey: String,
                                eventValue: String,
                                eventClient: String,
                                eventDate: String
                               )

object JSONCollectorPayload {
  implicit val formats = Json.format[JSONCollectorPayload]
  def write(cp: JSONCollectorPayload) = {
    Json.toJson(cp)
  }
  def read(jcp: JsValue) = {
    jcp.as[JSONCollectorPayload]
  }
}
