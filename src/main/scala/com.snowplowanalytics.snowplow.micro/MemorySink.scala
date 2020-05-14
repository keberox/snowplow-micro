/*
 * Copyright (c) 2019-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.micro

import com.snowplowanalytics.snowplow.collectors.scalastream.sinks.Sink
import com.snowplowanalytics.snowplow.enrich.common.loaders.ThriftLoader
import com.snowplowanalytics.snowplow.enrich.common.adapters.{
  AdapterRegistry,
  RawEvent
}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EnrichmentManager, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils
import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils.decodeBase64Url
import com.snowplowanalytics.snowplow.enrich.common.utils.shredder.Shredder
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ValidatableJsonMethods._
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import scalaz.{Validation, Success, Failure}
import com.fasterxml.jackson.databind.JsonNode
import scala.collection.JavaConverters._

/** Sink of the collector that Snowplow Micro is.
  * Contains the functions that are called for each event sent
  * to the collector endpoint.
  * For each event received, it tries to validate it using scala-common-enrich,
  * and then stores the results in memory in [[ValidationCache]].
  * The events are received as [[CollectorPayload]]s serialized with Thrift.
  */
private[micro] final case class MemorySink(resolver: Resolver) extends Sink {
  implicit val resolv = resolver

  val MaxBytes = Long.MaxValue

  /** Function of the [[Sink]] called for all the events received by a collector. */
  override def storeRawEvents(events: List[Array[Byte]], key: String) = {
    events.foreach(bytes => processThriftBytes(bytes, resolver))
    Nil
  }

  val EnrichmentReg = new EnrichmentRegistry(Map())

  /** Deserialize Thrift bytes into [[CollectorPayload]]s,
    * validate them and store the result in [[ValidationCache]].
    * A [[CollectorPayload]] can contain several events.
    */
  private[micro] def processThriftBytes(
    thriftBytes: Array[Byte],
    resolver: Resolver
  ): Unit =
    ThriftLoader.toCollectorPayload(thriftBytes) match {
      case Success(maybePayload) =>
        maybePayload match {
          case Some(collectorPayload) =>
            AdapterRegistry.toRawEvents(collectorPayload) match {
              case Success(rawEvents) =>
                val (good, bad) = rawEvents.list.foldRight((Nil, Nil) : (List[GoodEvent], List[BadEvent])) {
                  case (rawEvent, (good, bad)) =>
                    extractEventInfo(rawEvent) match {
                      case Success(goodEvent) =>
                        (goodEvent :: good, bad)
                      case Failure(error) =>
                        val badEvent = BadEvent(Some(collectorPayload), Some(rawEvent), List(error))
                        (good, badEvent :: bad)
                    }
                }

                ValidationCache.addToGood(good)
                ValidationCache.addToBad(bad)

              case Failure(errors) =>
                val bad = BadEvent(Some(collectorPayload), None, List("Error while extracting event(s) from collector payload and validating it/them.") ++ errors.list)
                ValidationCache.addToBad(List(bad))
            }
          case None =>
            val bad = BadEvent(None, None, List("No payload."))
            ValidationCache.addToBad(List(bad))
        }
      case Failure(errors) =>
        val bad = BadEvent(None, None, List("Can't deserialize Thrift bytes.") ++ errors.list)
        ValidationCache.addToBad(List(bad))
    }

  private[micro] def extractEventInfo(event: RawEvent): Validation[String, GoodEvent] =
    EnrichmentManager.enrichEvent(EnrichmentReg, "micro", new DateTime(System.currentTimeMillis), event) match {
      case Success(enriched) =>
        Shredder.extractAndValidateCustomContexts(enriched) match {
          case Success(pairs) =>
            val contexts = pairs.map(_._1.toSchemaUri)
            Success(GoodEvent(event, getEventType(event), Option(enriched.event), Some(contexts)))
          case Failure(f) =>
            // This failure should not happen because event has already passed enrichment.
            Failure(s"Error while extracting custom contexts: $f")
        }
      case Failure(f) => Failure(s"Error while validating the event: $f")
    }

  /** Extract the event type of an event if any (in param "e"). */
  private[micro] def getEventType(event: RawEvent): Option[String] =
    event.parameters.get("e")

}
