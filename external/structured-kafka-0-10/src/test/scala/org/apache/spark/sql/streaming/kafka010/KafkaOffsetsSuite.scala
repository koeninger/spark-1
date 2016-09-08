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

package org.apache.spark.sql.streaming.kafka010

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkFunSuite

class KafkaOffsetsSuite extends SparkFunSuite {
  val tpA0 = new TopicPartition("a", 0)
  val tpA1 = new TopicPartition("a", 1)
  val tpB0 = new TopicPartition("b", 0)
  val tpB1 = new TopicPartition("b", 1)

  test("equal") {
    List(
      KafkaOffsets(Map()) ->
        KafkaOffsets(Map()),
      KafkaOffsets(Map(tpA0 -> 1, tpA1 -> 2, tpB0 -> 3, tpB1 -> 4)) ->
        KafkaOffsets(Map(tpA0 -> 1, tpA1 -> 2, tpB0 -> 3, tpB1 -> 4))
    ).foreach { pair =>
      assert(0 == pair._1.compareTo(pair._2), pair)
    }
  }

  test("greater than") {
    List(
      KafkaOffsets(Map(tpA0 -> 2, tpA1 -> 1)) ->
        KafkaOffsets(Map(tpA0 -> 1, tpA1 -> 1)),
      KafkaOffsets(Map(tpA1 -> 2, tpB0 -> 0)) ->
        KafkaOffsets(Map(tpA0 -> 1, tpA1 -> 1))
    ).foreach { pair =>
      assert(1 == pair._1.compareTo(pair._2), pair)
    }
  }

  test("less than") {
    List(
      KafkaOffsets(Map(tpA0 -> 1, tpA1 -> 1)) ->
        KafkaOffsets(Map(tpA0 -> 2, tpA1 -> 1)),
      KafkaOffsets(Map(tpA0 -> 1, tpA1 -> 1)) ->
        KafkaOffsets(Map(tpA1 -> 2, tpB0 -> 0))
    ).foreach { pair =>
      assert(-1 == pair._1.compareTo(pair._2), pair)
    }
  }

  test("different") {
    List(
      KafkaOffsets(Map(tpA0 -> 1, tpA1 -> 1)) ->
        KafkaOffsets(Map(tpB0 -> 1, tpB1 -> 1)),
      KafkaOffsets(Map(tpA0 -> 1, tpA1 -> 1)) ->
        KafkaOffsets(Map(tpA1 -> 1, tpB0 -> 1))
    ).foreach { pair =>
      assert(pair._1.compareTo(pair._2) != pair._2.compareTo(pair._1), pair)
    }
  }

}
