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

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

public class ColumnarBatchRowIteratorTest {

  private KeyValueBatch createBatch(int n) {
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
      vals[i] = ("v" + i).getBytes(UTF_8);
    }
    return new KeyValueBatch(rows, cfs, cqs, cvs, ts, del, vals, n);
  }

  @Test
  public void testIterateAllEntries() {
    KeyValueBatch batch = createBatch(5);
    ColumnarBatchRowIterator iter = new ColumnarBatchRowIterator(batch);

    List<String> rows = new ArrayList<>();
    while (iter.hasNext()) {
      iter.advance();
      rows.add(new String(iter.currentKey().getRowData().toArray(), UTF_8));
    }

    assertEquals(List.of("r0", "r1", "r2", "r3", "r4"), rows);
  }

  @Test
  public void testIterateWithDeselected() {
    KeyValueBatch batch = createBatch(5);
    batch.deselect(1);
    batch.deselect(3);

    ColumnarBatchRowIterator iter = new ColumnarBatchRowIterator(batch);

    List<String> rows = new ArrayList<>();
    while (iter.hasNext()) {
      iter.advance();
      rows.add(new String(iter.currentKey().getRowData().toArray(), UTF_8));
    }

    assertEquals(List.of("r0", "r2", "r4"), rows);
  }

  @Test
  public void testIterateAllDeselected() {
    KeyValueBatch batch = createBatch(3);
    batch.deselect(0);
    batch.deselect(1);
    batch.deselect(2);

    ColumnarBatchRowIterator iter = new ColumnarBatchRowIterator(batch);
    assertFalse(iter.hasNext());
  }

  @Test
  public void testEmptyBatch() {
    KeyValueBatch batch = new KeyValueBatch(new byte[0][], new byte[0][], new byte[0][],
        new byte[0][], new long[0], new boolean[0], new byte[0][], 0);
    ColumnarBatchRowIterator iter = new ColumnarBatchRowIterator(batch);
    assertFalse(iter.hasNext());
  }

  @Test
  public void testGetLastKey() {
    KeyValueBatch batch = createBatch(3);
    batch.deselect(2); // deselect last entry

    ColumnarBatchRowIterator iter = new ColumnarBatchRowIterator(batch);
    // getLastKey() returns the batch's last key regardless of selection
    Key lastKey = iter.getLastKey();
    assertArrayEquals("r2".getBytes(UTF_8), lastKey.getRowData().toArray());
  }

  @Test
  public void testCurrentValue() {
    KeyValueBatch batch = createBatch(3);
    ColumnarBatchRowIterator iter = new ColumnarBatchRowIterator(batch);

    assertTrue(iter.hasNext());
    iter.advance();
    assertArrayEquals("v0".getBytes(UTF_8), iter.currentValue().get());

    iter.advance();
    assertArrayEquals("v1".getBytes(UTF_8), iter.currentValue().get());
  }

  @Test
  public void testBitScanAcrossWordBoundary() {
    // 130 entries spanning 3 long words — deselect all except entries at word boundaries
    KeyValueBatch batch = createBatch(130);
    for (int i = 0; i < 130; i++) {
      batch.deselect(i);
    }
    // Re-select just a few at interesting positions (we deselected then can't re-select,
    // so instead create a fresh batch and deselect everything except targets)
    batch = createBatch(130);
    for (int i = 0; i < 130; i++) {
      if (i != 0 && i != 63 && i != 64 && i != 129) {
        batch.deselect(i);
      }
    }

    ColumnarBatchRowIterator iter = new ColumnarBatchRowIterator(batch);
    List<String> rows = new ArrayList<>();
    while (iter.hasNext()) {
      iter.advance();
      rows.add(new String(iter.currentKey().getRowData().toArray(), UTF_8));
    }

    assertEquals(List.of("r0", "r63", "r64", "r129"), rows);
  }

  @Test
  public void testBitScanSparseSelection() {
    // Large batch where only 1 entry per word is selected — tests efficient skip
    KeyValueBatch batch = createBatch(200);
    for (int i = 0; i < 200; i++) {
      if (i % 64 != 0) {
        batch.deselect(i);
      }
    }

    ColumnarBatchRowIterator iter = new ColumnarBatchRowIterator(batch);
    List<String> rows = new ArrayList<>();
    while (iter.hasNext()) {
      iter.advance();
      rows.add(new String(iter.currentKey().getRowData().toArray(), UTF_8));
    }

    assertEquals(List.of("r0", "r64", "r128", "r192"), rows);
  }
}
