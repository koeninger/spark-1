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

import scala.concurrent.duration._

import org.apache.spark.sql.streaming.{ ProcessingTime, StreamTest }

class KafkaSourceSuite extends StreamTest {
  import testImplicits._

  test("bogus") {
    val lines = spark.readStream.format("org.apache.spark.sql.streaming.kafka010.KafkaSourceProvider").load()
    val query = lines.writeStream.outputMode("append").format("console").trigger(ProcessingTime(5.seconds)).start()

    query.awaitTermination()
  }
}
