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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

public class KeyValueBatchTest {

  private KeyValueBatch createTestBatch(int numEntries) {
    byte[][] rows = new byte[numEntries][];
    byte[][] cfs = new byte[numEntries][];
    byte[][] cqs = new byte[numEntries][];
    byte[][] cvs = new byte[numEntries][];
    long[] timestamps = new long[numEntries];
    boolean[] deleted = new boolean[numEntries];
    byte[][] values = new byte[numEntries][];

    for (int i = 0; i < numEntries; i++) {
      rows[i] = ("row" + i).getBytes(UTF_8);
      cfs[i] = ("cf" + i).getBytes(UTF_8);
      cqs[i] = ("cq" + i).getBytes(UTF_8);
      cvs[i] = ("cv" + i).getBytes(UTF_8);
      timestamps[i] = 1000L + i;
      deleted[i] = (i % 5 == 0);
      values[i] = ("val" + i).getBytes(UTF_8);
    }

    return new KeyValueBatch(rows, cfs, cqs, cvs, timestamps, deleted, values, numEntries);
  }

  @Test
  public void testBasicAccess() {
    KeyValueBatch batch = createTestBatch(5);

    assertEquals(5, batch.getNumEntries());
    assertEquals(5, batch.getNumSelected());
    assertArrayEquals("row0".getBytes(UTF_8), batch.getRow(0));
    assertArrayEquals("cf2".getBytes(UTF_8), batch.getColFamily(2));
    assertArrayEquals("cq3".getBytes(UTF_8), batch.getColQualifier(3));
    assertArrayEquals("cv4".getBytes(UTF_8), batch.getColVisibility(4));
    assertEquals(1001L, batch.getTimestamp(1));
    assertTrue(batch.isDeleted(0));
    assertFalse(batch.isDeleted(1));
    assertArrayEquals("val3".getBytes(UTF_8), batch.getValue(3));
  }

  @Test
  public void testSelectionVector() {
    KeyValueBatch batch = createTestBatch(5);

    assertTrue(batch.isSelected(0));
    assertTrue(batch.isSelected(1));
    assertEquals(5, batch.getNumSelected());

    batch.deselect(1);
    assertFalse(batch.isSelected(1));
    assertEquals(4, batch.getNumSelected());

    // Deselecting again should be idempotent
    batch.deselect(1);
    assertFalse(batch.isSelected(1));
    assertEquals(4, batch.getNumSelected());

    batch.deselect(3);
    assertEquals(3, batch.getNumSelected());
  }

  @Test
  public void testMaterializeKey() {
    KeyValueBatch batch = createTestBatch(3);
    Key key = batch.materializeKey(1);

    assertArrayEquals("row1".getBytes(UTF_8), key.getRowData().toArray());
    assertArrayEquals("cf1".getBytes(UTF_8), key.getColumnFamilyData().toArray());
    assertArrayEquals("cq1".getBytes(UTF_8), key.getColumnQualifierData().toArray());
    assertArrayEquals("cv1".getBytes(UTF_8), key.getColumnVisibilityData().toArray());
    assertEquals(1001L, key.getTimestamp());
    assertFalse(key.isDeleted());
  }

  @Test
  public void testMaterializeValue() {
    KeyValueBatch batch = createTestBatch(3);
    Value val = batch.materializeValue(2);
    assertArrayEquals("val2".getBytes(UTF_8), val.get());
  }

  @Test
  public void testGetLastKey() {
    KeyValueBatch batch = createTestBatch(3);
    Key lastKey = batch.getLastKey();
    assertArrayEquals("row2".getBytes(UTF_8), lastKey.getRowData().toArray());
    assertEquals(1002L, lastKey.getTimestamp());
  }

  @Test
  public void testEmptyBatch() {
    KeyValueBatch batch = new KeyValueBatch(new byte[0][], new byte[0][], new byte[0][],
        new byte[0][], new long[0], new boolean[0], new byte[0][], 0);
    assertEquals(0, batch.getNumEntries());
    assertEquals(0, batch.getNumSelected());
    assertEquals(null, batch.getLastKey());
    assertEquals(0, batch.getVisDictSize());
    assertEquals(0, batch.getSelectionMaskLength());
  }

  // --- Visibility dictionary tests ---

  @Test
  public void testVisDictSingleVisibility() {
    int n = 5;
    byte[][] rows = new byte[n][];
    byte[][] cfs = new byte[n][];
    byte[][] cqs = new byte[n][];
    byte[][] cvs = new byte[n][];
    long[] ts = new long[n];
    boolean[] del = new boolean[n];
    byte[][] vals = new byte[n][];
    for (int i = 0; i < n; i++) {
      rows[i] = ("row" + i).getBytes(UTF_8);
      cfs[i] = "cf".getBytes(UTF_8);
      cqs[i] = "cq".getBytes(UTF_8);
      cvs[i] = "A".getBytes(UTF_8);
      ts[i] = i;
      del[i] = false;
      vals[i] = "v".getBytes(UTF_8);
    }
    KeyValueBatch batch = new KeyValueBatch(rows, cfs, cqs, cvs, ts, del, vals, n);

    assertEquals(1, batch.getVisDictSize());
    assertArrayEquals("A".getBytes(UTF_8), batch.getVisDictEntry(0));
    for (int i = 0; i < n; i++) {
      assertEquals(0, batch.getVisDictId(i));
    }
  }

  @Test
  public void testVisDictMultipleVisibilities() {
    int n = 6;
    byte[][] rows = new byte[n][];
    byte[][] cfs = new byte[n][];
    byte[][] cqs = new byte[n][];
    byte[][] cvs = new byte[n][];
    long[] ts = new long[n];
    boolean[] del = new boolean[n];
    byte[][] vals = new byte[n][];
    String[] vises = {"A", "B", "A", "C", "B", "A"};
    for (int i = 0; i < n; i++) {
      rows[i] = ("row" + i).getBytes(UTF_8);
      cfs[i] = "cf".getBytes(UTF_8);
      cqs[i] = "cq".getBytes(UTF_8);
      cvs[i] = vises[i].getBytes(UTF_8);
      ts[i] = i;
      del[i] = false;
      vals[i] = "v".getBytes(UTF_8);
    }
    KeyValueBatch batch = new KeyValueBatch(rows, cfs, cqs, cvs, ts, del, vals, n);

    assertEquals(3, batch.getVisDictSize());
    // Entries with same visibility should share the same dict ID
    assertEquals(batch.getVisDictId(0), batch.getVisDictId(2)); // both "A"
    assertEquals(batch.getVisDictId(0), batch.getVisDictId(5)); // both "A"
    assertEquals(batch.getVisDictId(1), batch.getVisDictId(4)); // both "B"
    // Different visibilities should have different dict IDs
    assertTrue(batch.getVisDictId(0) != batch.getVisDictId(1)); // A != B
    assertTrue(batch.getVisDictId(0) != batch.getVisDictId(3)); // A != C
    assertTrue(batch.getVisDictId(1) != batch.getVisDictId(3)); // B != C
  }

  @Test
  public void testVisDictEmptyVisibility() {
    int n = 3;
    byte[][] rows = new byte[n][];
    byte[][] cfs = new byte[n][];
    byte[][] cqs = new byte[n][];
    byte[][] cvs = new byte[n][];
    long[] ts = new long[n];
    boolean[] del = new boolean[n];
    byte[][] vals = new byte[n][];
    cvs[0] = new byte[0];
    cvs[1] = "A".getBytes(UTF_8);
    cvs[2] = new byte[0];
    for (int i = 0; i < n; i++) {
      rows[i] = ("row" + i).getBytes(UTF_8);
      cfs[i] = "cf".getBytes(UTF_8);
      cqs[i] = "cq".getBytes(UTF_8);
      ts[i] = i;
      del[i] = false;
      vals[i] = "v".getBytes(UTF_8);
    }
    KeyValueBatch batch = new KeyValueBatch(rows, cfs, cqs, cvs, ts, del, vals, n);

    assertEquals(2, batch.getVisDictSize());
    assertEquals(batch.getVisDictId(0), batch.getVisDictId(2)); // both empty
    assertTrue(batch.getVisDictId(0) != batch.getVisDictId(1)); // empty != "A"
  }

  // --- Bitmask selection tests ---

  @Test
  public void testAndSelectionMask() {
    KeyValueBatch batch = createTestBatch(5);
    assertEquals(5, batch.getNumSelected());

    // Create a mask that clears entries 1 and 3
    long[] mask = new long[batch.getSelectionMaskLength()];
    mask[0] = ~0L;
    mask[0] &= ~(1L << 1); // clear bit 1
    mask[0] &= ~(1L << 3); // clear bit 3

    batch.andSelectionMask(mask);

    assertTrue(batch.isSelected(0));
    assertFalse(batch.isSelected(1));
    assertTrue(batch.isSelected(2));
    assertFalse(batch.isSelected(3));
    assertTrue(batch.isSelected(4));
    assertEquals(3, batch.getNumSelected());
  }

  @Test
  public void testSelectionMaskWordBoundary() {
    // Test with > 64 entries to exercise multi-word behavior
    int n = 130;
    byte[][] rows = new byte[n][];
    byte[][] cfs = new byte[n][];
    byte[][] cqs = new byte[n][];
    byte[][] cvs = new byte[n][];
    long[] ts = new long[n];
    boolean[] del = new boolean[n];
    byte[][] vals = new byte[n][];
    for (int i = 0; i < n; i++) {
      rows[i] = ("r" + i).getBytes(UTF_8);
      cfs[i] = "cf".getBytes(UTF_8);
      cqs[i] = "cq".getBytes(UTF_8);
      cvs[i] = new byte[0];
      ts[i] = i;
      del[i] = false;
      vals[i] = "v".getBytes(UTF_8);
    }
    KeyValueBatch batch = new KeyValueBatch(rows, cfs, cqs, cvs, ts, del, vals, n);

    assertEquals(n, batch.getNumSelected());
    assertEquals(3, batch.getSelectionMaskLength()); // ceil(130/64) = 3

    // Deselect entries at word boundaries
    batch.deselect(0);
    batch.deselect(63);
    batch.deselect(64);
    batch.deselect(127);
    batch.deselect(128);
    batch.deselect(129);

    assertFalse(batch.isSelected(0));
    assertTrue(batch.isSelected(1));
    assertTrue(batch.isSelected(62));
    assertFalse(batch.isSelected(63));
    assertFalse(batch.isSelected(64));
    assertTrue(batch.isSelected(65));
    assertTrue(batch.isSelected(126));
    assertFalse(batch.isSelected(127));
    assertFalse(batch.isSelected(128));
    assertFalse(batch.isSelected(129));
    assertEquals(n - 6, batch.getNumSelected());
  }
}
