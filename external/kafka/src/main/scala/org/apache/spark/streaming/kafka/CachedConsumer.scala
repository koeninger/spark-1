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

package org.apache.spark.streaming.kafka

import kafka.consumer.SimpleConsumer
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

class CachedConsumer private (
    host: String,
    port: Int,
    soTimeout: Int,
    bufferSize: Int,
    clientId: String) extends SimpleConsumer(host, port, soTimeout, bufferSize, clientId) {

  override def close() {
    // superclass implementation of sendRequest will attempt to reconnect on 1 failure,
    // so just leave connections permanently open without checking connection status
  }
}

object CachedConsumer {
  // tuple of all args to connect, to make sure cached connection matches requested config
  private type Key = (String, Int, Int, Int, String)

  // SimpleConsumer is marked as threadsafe, so just cache them rather than pooling.
  private val cache: LoadingCache[Key, CachedConsumer] = CacheBuilder.newBuilder()
    .build(
    new CacheLoader[Key, CachedConsumer]() {
      def load(key: Key) = {
        val (host, port, soTimeout, bufferSize, clientId) = key
        new CachedConsumer(host, port, soTimeout, bufferSize, clientId)
      }
    })

  def apply(
      host: String,
      port: Int,
      soTimeout: Int,
      bufferSize: Int,
      clientId: String): CachedConsumer = cache.get((host, port, soTimeout, bufferSize, clientId))

  def invalidate(cc: CachedConsumer): Unit = {
    CachedConsumer(cc.host, cc.port, cc.soTimeout, cc.bufferSize, cc.clientId).close()
    cache.invalidate((cc.host, cc.port, cc.soTimeout, cc.bufferSize, cc.clientId))
  }

}
