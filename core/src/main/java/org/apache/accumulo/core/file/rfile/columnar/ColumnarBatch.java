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
 * Abstract base class for a batch of Key/Value entries supporting bitmask-based selection filtering
 * and a visibility dictionary. Concrete implementations differ in how they store entry data:
 * <ul>
 * <li>{@link KeyValueBatch} — decomposed columnar byte arrays (for raw block reads)</li>
 * <li>{@link KeyRefBatch} — Key/Value object references (for iterator-sourced reads)</li>
 * </ul>
 *
 * <p>
 * Entries can be deselected (filtered out) via a bitmask selection vector without compacting or
 * copying data. Filters modify the selection vector either per-entry via {@link #deselect(int)} or
 * at the word level via {@link #andSelectionMask(long[])}.
 *
 * <p>
 * Column visibilities are dictionary-encoded at construction time: each unique visibility byte
 * array is assigned a small integer ID (0..D-1).
 *
 * @since 2.1.5
 */
public abstract class ColumnarBatch {

  private final int numEntries;

  // Bitmask selection vector: bit i set = entry i selected
  private final long[] selected;
  private final int numWords;

  // Visibility dictionary: maps each entry to a small int ID for its unique visibility
  private final int[] visDictIds;
  private final byte[][] visDict;
  private final int visDictSize;

  /** Package-private container for pre-built visibility dictionary state. */
  static class VisDict {
    final int[] ids;
    final byte[][] entries;
    final int size;

    VisDict(int[] ids, byte[][] entries, int size) {
      this.ids = ids;
      this.entries = entries;
      this.size = size;
    }
  }

  ColumnarBatch(int numEntries, VisDict dict) {
    this.numEntries = numEntries;

    // Initialize selection mask with all entries selected
    this.numWords = (numEntries + 63) >>> 6;
    this.selected = new long[numWords];
    if (numEntries > 0) {
      Arrays.fill(selected, ~0L);
      // Clear trailing bits in the last word beyond numEntries
      int trailing = numEntries & 63;
      if (trailing != 0) {
        selected[numWords - 1] = (1L << trailing) - 1;
      }
    }

    this.visDictIds = dict.ids;
    this.visDict = dict.entries;
    this.visDictSize = dict.size;
  }

  public int getNumEntries() {
    return numEntries;
  }

  // --- Selection vector: per-entry API ---

  public boolean isSelected(int i) {
    return (selected[i >>> 6] & (1L << (i & 63))) != 0;
  }

  public void deselect(int i) {
    selected[i >>> 6] &= ~(1L << (i & 63));
  }

  public int getNumSelected() {
    int count = 0;
    for (int w = 0; w < numWords; w++) {
      count += Long.bitCount(selected[w]);
    }
    return count;
  }

  // --- Selection vector: word-level bitmask API ---

  public int getSelectionMaskLength() {
    return numWords;
  }

  public long getSelectionWord(int w) {
    return selected[w];
  }

  public void andSelectionMask(long[] mask) {
    for (int w = 0; w < numWords; w++) {
      selected[w] &= mask[w];
    }
  }

  // --- Visibility dictionary ---

  public int getVisDictId(int i) {
    return visDictIds[i];
  }

  public byte[] getVisDictEntry(int d) {
    return visDict[d];
  }

  public int getVisDictSize() {
    return visDictSize;
  }

  // --- Abstract entry data accessors ---

  public abstract byte[] getRow(int i);

  public abstract byte[] getColFamily(int i);

  public abstract byte[] getColQualifier(int i);

  public abstract byte[] getColVisibility(int i);

  public abstract long getTimestamp(int i);

  public abstract boolean isDeleted(int i);

  public abstract byte[] getValue(int i);

  // --- Abstract materialization ---

  /**
   * Returns the Key for entry i. The returned Key may reference internal batch data directly and
   * should not be modified by the caller. It is only valid while this batch is alive.
   */
  public abstract Key materializeKey(int i);

  /**
   * Returns the Value for entry i. The returned Value may reference internal batch data directly
   * and should not be modified by the caller.
   */
  public abstract Value materializeValue(int i);

  public abstract Key getLastKey();
}
