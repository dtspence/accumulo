/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.file.rfile.columnar;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Iterates over the selected entries in a {@link ColumnarBatch}, producing Key/Value pairs one at a
 * time. This is the bridge between columnar batch processing and row-based iteration.
 *
 * <p>
 * Uses bit-scanning ({@link Long#numberOfTrailingZeros}) on the batch's bitmask selection vector to
 * skip deselected entries efficiently — 64 entries per word when all are deselected.
 *
 * <p>
 * Usage pattern: {@code while (hasNext()) { advance(); use currentKey()/currentValue(); }}
 *
 * <p>
 * The iterator starts before the first entry. Call {@link #advance()} to position at the first
 * selected entry.
 *
 * @since 2.1.5
 */
public class ColumnarBatchRowIterator {

  private final ColumnarBatch batch;
  private int cursor;
  private int nextCursor;

  public ColumnarBatchRowIterator(ColumnarBatch batch) {
    this.batch = batch;
    this.cursor = -1;
    this.nextCursor = nextSelected(0);
  }

  /**
   * Returns true if there is another selected entry after the current cursor position.
   */
  public boolean hasNext() {
    return nextCursor >= 0;
  }

  /**
   * Advances to the next selected entry. Must only be called when {@link #hasNext()} is true.
   */
  public void advance() {
    cursor = nextCursor;
    nextCursor = nextSelected(cursor + 1);
  }

  /**
   * Returns the Key at the current position. The returned Key does not copy byte arrays and is only
   * valid until the next call to {@link #advance()} or until the batch is discarded.
   */
  public Key currentKey() {
    return batch.materializeKey(cursor);
  }

  /**
   * Returns the Value at the current position.
   */
  public Value currentValue() {
    return batch.materializeValue(cursor);
  }

  /**
   * Returns the Key of the last entry in the batch (regardless of selection). Used by
   * LocalityGroupReader to set prevKey correctly when the batch is exhausted.
   */
  public Key getLastKey() {
    return batch.getLastKey();
  }

  /**
   * Find the next set bit at or after {@code fromIndex}, or -1 if none exists within the batch.
   */
  private int nextSelected(int fromIndex) {
    int numEntries = batch.getNumEntries();
    if (fromIndex >= numEntries) {
      return -1;
    }
    int w = fromIndex >>> 6;
    // Mask off bits below fromIndex within the current word
    long word = batch.getSelectionWord(w) & (~0L << (fromIndex & 63));
    while (true) {
      if (word != 0) {
        int bit = (w << 6) + Long.numberOfTrailingZeros(word);
        return bit < numEntries ? bit : -1;
      }
      w++;
      if (w >= batch.getSelectionMaskLength()) {
        return -1;
      }
      word = batch.getSelectionWord(w);
    }
  }
}
