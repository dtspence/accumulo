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

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RelativeKey;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Builds {@link ColumnarBatch} instances from either raw RFile block streams or
 * {@link SortedKeyValueIterator} sources.
 *
 * @since 2.1.5
 */
public class KeyValueBatchBuilder {

  /**
   * Reads exactly {@code maxEntries} entries from a block into a columnar batch backed by
   * decomposed byte arrays.
   *
   * <p>
   * The caller must ensure {@code maxEntries} matches the actual number of remaining entries in the
   * block (as reported by the block index). If the block contains fewer entries than requested,
   * {@code readFields()} will throw {@link java.io.EOFException}.
   *
   * @param block the block data stream, positioned at the first entry to read
   * @param prevKey the previous key (needed for RelativeKey prefix decompression); may be a new
   *        Key() for the first block
   * @param maxEntries exact number of entries to read — must match the block's entry count
   * @return a populated ColumnarBatch
   * @throws IOException if the block stream cannot be read or contains fewer than maxEntries
   */
  public static ColumnarBatch readBlock(DataInput block, Key prevKey, int maxEntries)
      throws IOException {
    byte[][] rows = new byte[maxEntries][];
    byte[][] colFamilies = new byte[maxEntries][];
    byte[][] colQualifiers = new byte[maxEntries][];
    byte[][] colVisibilities = new byte[maxEntries][];
    long[] timestamps = new long[maxEntries];
    boolean[] deleted = new boolean[maxEntries];
    byte[][] values = new byte[maxEntries][];

    RelativeKey rk = new RelativeKey();
    rk.setPrevKey(prevKey);
    Value val = new Value();

    for (int i = 0; i < maxEntries; i++) {
      rk.readFields(block);
      val.readFields(block);

      Key key = rk.getKey();
      rows[i] = key.getRowData().toArray();
      colFamilies[i] = key.getColumnFamilyData().toArray();
      colQualifiers[i] = key.getColumnQualifierData().toArray();
      colVisibilities[i] = key.getColumnVisibilityData().toArray();
      timestamps[i] = key.getTimestamp();
      deleted[i] = key.isDeleted();
      values[i] = val.get();
    }

    return new KeyValueBatch(rows, colFamilies, colQualifiers, colVisibilities, timestamps, deleted,
        values, maxEntries);
  }

  /**
   * Reads up to {@code maxEntries} entries from a {@link SortedKeyValueIterator} into a batch
   * backed by Key/Value references. The source must have a top entry
   * ({@code source.hasTop() == true}) when called. The source is advanced past all consumed
   * entries.
   *
   * @param source the iterator to read from, positioned at the first entry to include
   * @param maxEntries maximum number of entries to read
   * @return a populated ColumnarBatch
   */
  public static ColumnarBatch readFromIterator(SortedKeyValueIterator<Key,Value> source,
      int maxEntries) throws IOException {
    Key[] keys = new Key[maxEntries];
    Value[] values = new Value[maxEntries];

    int count = 0;
    while (count < maxEntries && source.hasTop()) {
      keys[count] = source.getTopKey();
      // Snapshot the value: RFile reader reuses the Value object (but readFields() replaces
      // the internal byte[] each time, so .get() returns a fresh array per call)
      values[count] = new Value(source.getTopValue().get());
      count++;

      source.next();
    }

    if (count < maxEntries) {
      keys = Arrays.copyOf(keys, count);
      values = Arrays.copyOf(values, count);
    }

    return new KeyRefBatch(keys, values, count);
  }
}
