/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetUtils;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetStorageWriter {
  private static final Logger log = LoggerFactory.getLogger(OffsetStorageWriter.class);

  private final OffsetBackingStore backingStore;
  private final Converter keyConverter;
  private final Converter valueConverter;
  private final String namespace;
  // Offset data in Connect format
  private Map<Map<String, Object>, Map<String, Object>> data = new HashMap<>();

  private Map<Map<String, Object>, Map<String, Object>> toFlush = null;
  private final Semaphore flushInProgress = new Semaphore(1);
  // Unique ID for each flush request to handle callbacks after timeouts
  private long currentFlushId = 0;

  public OffsetStorageWriter(
      OffsetBackingStore backingStore,
      String namespace,
      Converter keyConverter,
      Converter valueConverter) {
    this.backingStore = backingStore;
    this.namespace = namespace;
    this.keyConverter = keyConverter;
    this.valueConverter = valueConverter;
  }

  @SuppressWarnings("unchecked")
  public synchronized void offset(Map<String, ?> partition, Map<String, ?> offset) {
    data.put((Map<String, Object>) partition, (Map<String, Object>) offset);
  }

  private boolean flushing() {
    return toFlush != null;
  }

  public boolean beginFlush() {
    try {
      return beginFlush(0, TimeUnit.NANOSECONDS);
    } catch (InterruptedException | TimeoutException e) {
      log.error(
          "Invalid call to OffsetStorageWriter beginFlush() while already flushing, the "
              + "framework should not allow this", e);
      throw new ConnectException("OffsetStorageWriter is already flushing");
    }
  }

  public boolean beginFlush(long timeout, TimeUnit timeUnit)
      throws InterruptedException, TimeoutException {
    if (flushInProgress.tryAcquire(Math.max(0, timeout), timeUnit)) {
      synchronized (this) {
        if (data.isEmpty()) {
          flushInProgress.release();
          return false;
        } else {
          toFlush = data;
          data = new HashMap<>();
          return true;
        }
      }
    } else {
      throw new TimeoutException("Timed out waiting for previous flush to finish");
    }
  }

  public Future<Void> doFlush(final Callback<Void> callback) {

    final long flushId;
    // Serialize
    final Map<ByteBuffer, ByteBuffer> offsetsSerialized;

    synchronized (this) {
      flushId = currentFlushId;

      try {
        offsetsSerialized = new HashMap<>(toFlush.size());
        for (Map.Entry<Map<String, Object>, Map<String, Object>> entry : toFlush.entrySet()) {
          // Offsets are specified as schemaless to the converter, using whatever internal schema is
          // appropriate
          // for that data. The only enforcement of the format is here.
          OffsetUtils.validateFormat(entry.getKey());
          OffsetUtils.validateFormat(entry.getValue());
          // When serializing the key, we add in the namespace information so the key is [namespace,
          // real key]
          byte[] key =
              keyConverter.fromConnectData(
                  namespace, null, Arrays.asList(namespace, entry.getKey()));
          ByteBuffer keyBuffer = (key != null) ? ByteBuffer.wrap(key) : null;
          byte[] value = valueConverter.fromConnectData(namespace, null, entry.getValue());
          ByteBuffer valueBuffer = (value != null) ? ByteBuffer.wrap(value) : null;
          offsetsSerialized.put(keyBuffer, valueBuffer);
        }
      } catch (Throwable t) {
        // Must handle errors properly here or the writer will be left mid-flush forever and be
        // unable to make progress.
        log.error(
            "CRITICAL: Failed to serialize offset data, making it impossible to commit "
                + "offsets under namespace {}. This likely won't recover unless the "
                + "unserializable partition or offset information is overwritten.",
            namespace);
        log.error("Cause of serialization failure:", t);
        callback.onCompletion(t, null);
        return null;
      }

      // And submit the data
      log.debug(
          "Submitting {} entries to backing store. The offsets are: {}",
          offsetsSerialized.size(),
          toFlush);
    }

    return backingStore.set(
        offsetsSerialized,
        (error, result) -> {
          boolean isCurrent = handleFinishWrite(flushId, error);
          if (isCurrent && callback != null) {
            callback.onCompletion(error, result);
          }
        });
  }

  public synchronized void cancelFlush() {
    // Verify we're still flushing data to handle a race between cancelFlush() calls from up the
    // call stack and callbacks from the write request to underlying storage
    if (flushing()) {
      // Just recombine the data and place it back in the primary storage
      toFlush.putAll(data);
      data = toFlush;
      currentFlushId++;
      flushInProgress.release();
      toFlush = null;
    }
  }

  private synchronized boolean handleFinishWrite(long flushId, Throwable error) {
    // Callbacks need to be handled carefully since the flush operation may have already timed
    // out and been cancelled.
    if (flushId != currentFlushId) return false;

    if (error != null) {
      cancelFlush();
    } else {
      currentFlushId++;
      flushInProgress.release();
      toFlush = null;
    }
    return true;
  }
}
