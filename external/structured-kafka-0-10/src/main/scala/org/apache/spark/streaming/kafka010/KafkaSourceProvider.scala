/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.kafka010

import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.StructType

class KafkaSourceProvider extends StreamSourceProvider {
  def createSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]
  ): Source = {
    // TODO  Map[String, String] as lowest common denominator for config is fugly, better way?
    val kafkaParams: Map[String, Object] = parameters.collect {
      case ("key.deserializer", x) => ("key.deserializer", Class.forName(x))
      case ("value.deserializer", x) => ("value.deserializer", Class.forName(x))
      case (k, v) if k.contains(".") && !k.startsWith("spark") => (k, v)
    }
    // Comma is not valid in topic, so use it as delimiter, see kafka.common.Topic
    val topics = parameters("Subscribe").split(",")
    // TODO how to get appropriate types, strategies, deserializers
    new KafkaSource[String, String](
      sqlContext,
      PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParams)
    )
  }

  def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String,String]
  ): (String, StructType) = {
    ("KafkaSource", KafkaSource.stringSchema)
  }

}
