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

import java.util.Arrays;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * A columnar batch backed by decomposed byte arrays — one array per key field. Used when entries
 * are read from raw RFile data blocks where Key objects do not yet exist.
 *
 * @since 2.1.5
 * @see ColumnarBatch
 */
public class KeyValueBatch extends ColumnarBatch {

  private final byte[][] rows;
  private final byte[][] colFamilies;
  private final byte[][] colQualifiers;
  private final byte[][] colVisibilities;
  private final long[] timestamps;
  private final boolean[] deleted;
  private final byte[][] values;

  KeyValueBatch(byte[][] rows, byte[][] colFamilies, byte[][] colQualifiers,
      byte[][] colVisibilities, long[] timestamps, boolean[] deleted, byte[][] values,
      int numEntries) {
    super(numEntries, buildVisDict(colVisibilities, numEntries));
    this.rows = rows;
    this.colFamilies = colFamilies;
    this.colQualifiers = colQualifiers;
    this.colVisibilities = colVisibilities;
    this.timestamps = timestamps;
    this.deleted = deleted;
    this.values = values;
  }

  /**
   * Builds the visibility dictionary from decomposed visibility byte arrays. Returns a VisDict
   * containing the dictionary IDs, unique entries, and size.
   */
  static VisDict buildVisDict(byte[][] colVisibilities, int numEntries) {
    int[] visDictIds = new int[numEntries];
    byte[][] dict = new byte[Math.min(Math.max(numEntries, 1), 64)][];
    int dictCount = 0;
    for (int i = 0; i < numEntries; i++) {
      byte[] vis = colVisibilities[i];
      int dictId = -1;
      for (int d = 0; d < dictCount; d++) {
        if (Arrays.equals(vis, dict[d])) {
          dictId = d;
          break;
        }
      }
      if (dictId == -1) {
        dictId = dictCount;
        if (dictCount == dict.length) {
          dict = Arrays.copyOf(dict, dict.length * 2);
        }
        dict[dictCount++] = vis;
      }
      visDictIds[i] = dictId;
    }
    return new VisDict(visDictIds, Arrays.copyOf(dict, dictCount), dictCount);
  }

  @Override
  public byte[] getRow(int i) {
    return rows[i];
  }

  @Override
  public byte[] getColFamily(int i) {
    return colFamilies[i];
  }

  @Override
  public byte[] getColQualifier(int i) {
    return colQualifiers[i];
  }

  @Override
  public byte[] getColVisibility(int i) {
    return colVisibilities[i];
  }

  @Override
  public long getTimestamp(int i) {
    return timestamps[i];
  }

  @Override
  public boolean isDeleted(int i) {
    return deleted[i];
  }

  @Override
  public byte[] getValue(int i) {
    return values[i];
  }

  @Override
  public Key materializeKey(int i) {
    return new Key(rows[i], colFamilies[i], colQualifiers[i], colVisibilities[i], timestamps[i],
        deleted[i], false);
  }

  @Override
  public Value materializeValue(int i) {
    return new Value(values[i]);
  }

  @Override
  public Key getLastKey() {
    if (getNumEntries() == 0) {
      return null;
    }
    return materializeKey(getNumEntries() - 1);
  }
}
