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

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * A batch backed by Key/Value object references. Used when entries are read from an iterator source
 * that already provides materialized Key objects. Avoids the decompose-then-reassemble overhead of
 * {@link KeyValueBatch} — materialization returns the original Key/Value directly.
 *
 * @since 2.1.5
 * @see ColumnarBatch
 */
public class KeyRefBatch extends ColumnarBatch {

  private final Key[] keys;
  private final Value[] values;

  KeyRefBatch(Key[] keys, Value[] values, int numEntries) {
    super(numEntries, buildVisDict(keys, numEntries));
    this.keys = keys;
    this.values = values;
  }

  /**
   * Builds the visibility dictionary using ByteSequence views from Key objects. Only unique
   * visibility entries are copied via toArray() — per-entry access is zero-copy.
   */
  static VisDict buildVisDict(Key[] keys, int numEntries) {
    int[] visDictIds = new int[numEntries];
    ByteSequence[] dictRefs = new ByteSequence[Math.min(Math.max(numEntries, 1), 64)];
    byte[][] dict = new byte[dictRefs.length][];
    int dictCount = 0;
    for (int i = 0; i < numEntries; i++) {
      ByteSequence vis = keys[i].getColumnVisibilityData();
      int dictId = -1;
      for (int d = 0; d < dictCount; d++) {
        if (vis.equals(dictRefs[d])) {
          dictId = d;
          break;
        }
      }
      if (dictId == -1) {
        dictId = dictCount;
        if (dictCount == dictRefs.length) {
          dictRefs = Arrays.copyOf(dictRefs, dictRefs.length * 2);
          dict = Arrays.copyOf(dict, dictRefs.length);
        }
        dictRefs[dictCount] = vis;
        dict[dictCount] = vis.toArray();
        dictCount++;
      }
      visDictIds[i] = dictId;
    }
    return new VisDict(visDictIds, Arrays.copyOf(dict, dictCount), dictCount);
  }

  @Override
  public byte[] getRow(int i) {
    return keys[i].getRowData().toArray();
  }

  @Override
  public byte[] getColFamily(int i) {
    return keys[i].getColumnFamilyData().toArray();
  }

  @Override
  public byte[] getColQualifier(int i) {
    return keys[i].getColumnQualifierData().toArray();
  }

  @Override
  public byte[] getColVisibility(int i) {
    return keys[i].getColumnVisibilityData().toArray();
  }

  @Override
  public long getTimestamp(int i) {
    return keys[i].getTimestamp();
  }

  @Override
  public boolean isDeleted(int i) {
    return keys[i].isDeleted();
  }

  @Override
  public byte[] getValue(int i) {
    return values[i].get();
  }

  @Override
  public Key materializeKey(int i) {
    return keys[i];
  }

  @Override
  public Value materializeValue(int i) {
    return values[i];
  }

  @Override
  public Key getLastKey() {
    if (getNumEntries() == 0) {
      return null;
    }
    return keys[getNumEntries() - 1];
  }
}
