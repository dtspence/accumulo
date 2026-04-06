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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iteratorsImpl.ClientIteratorEnvironment;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;

public class KeyRefBatchTest {

  private KeyRefBatch createBatch(int numEntries, String... visibilities) {
    Key[] keys = new Key[numEntries];
    Value[] values = new Value[numEntries];
    for (int i = 0; i < numEntries; i++) {
      String vis = visibilities.length > 0 ? visibilities[i % visibilities.length] : "";
      keys[i] = new Key("row" + i, "cf" + i, "cq" + i, vis, 1000L + i);
      values[i] = new Value(("val" + i).getBytes(UTF_8));
    }
    return new KeyRefBatch(keys, values, numEntries);
  }

  @Test
  public void testBasicAccessors() {
    KeyRefBatch batch = createBatch(5, "A", "B");

    assertEquals(5, batch.getNumEntries());

    for (int i = 0; i < 5; i++) {
      assertArrayEquals(("row" + i).getBytes(UTF_8), batch.getRow(i));
      assertArrayEquals(("cf" + i).getBytes(UTF_8), batch.getColFamily(i));
      assertArrayEquals(("cq" + i).getBytes(UTF_8), batch.getColQualifier(i));
      assertEquals(1000L + i, batch.getTimestamp(i));
      assertFalse(batch.isDeleted(i));
      assertArrayEquals(("val" + i).getBytes(UTF_8), batch.getValue(i));
    }
  }

  @Test
  public void testMaterializeKeyReturnsOriginal() {
    Key[] keys = new Key[3];
    Value[] values = new Value[3];
    for (int i = 0; i < 3; i++) {
      keys[i] = new Key("row" + i, "cf", "cq", "", 1000L);
      values[i] = new Value(("v" + i).getBytes(UTF_8));
    }
    KeyRefBatch batch = new KeyRefBatch(keys, values, 3);

    // Core contract: materializeKey returns the exact same Key object, not a copy
    for (int i = 0; i < 3; i++) {
      assertSame(keys[i], batch.materializeKey(i));
    }
  }

  @Test
  public void testMaterializeValueReturnsOriginal() {
    Key[] keys = new Key[3];
    Value[] values = new Value[3];
    for (int i = 0; i < 3; i++) {
      keys[i] = new Key("row" + i, "cf", "cq", "", 1000L);
      values[i] = new Value(("v" + i).getBytes(UTF_8));
    }
    KeyRefBatch batch = new KeyRefBatch(keys, values, 3);

    for (int i = 0; i < 3; i++) {
      assertSame(values[i], batch.materializeValue(i));
    }
  }

  @Test
  public void testVisibilityDictionary() {
    String[] vis = {"A", "B", "C"};
    KeyRefBatch batch = createBatch(100, vis);

    // 3 unique visibilities across 100 entries
    assertEquals(3, batch.getVisDictSize());

    // Verify dictionary entries contain the correct bytes
    boolean foundA = false, foundB = false, foundC = false;
    for (int d = 0; d < 3; d++) {
      String entry = new String(batch.getVisDictEntry(d), UTF_8);
      if ("A".equals(entry)) {
        foundA = true;
      }
      if ("B".equals(entry)) {
        foundB = true;
      }
      if ("C".equals(entry)) {
        foundC = true;
      }
    }
    assertTrue(foundA && foundB && foundC);

    // Verify each entry maps to the correct dictionary ID
    for (int i = 0; i < 100; i++) {
      int dictId = batch.getVisDictId(i);
      String expected = vis[i % 3];
      assertArrayEquals(expected.getBytes(UTF_8), batch.getVisDictEntry(dictId));
    }
  }

  @Test
  public void testSelectionMask() {
    KeyRefBatch batch = createBatch(10, "A");

    // All selected initially
    assertEquals(10, batch.getNumSelected());
    for (int i = 0; i < 10; i++) {
      assertTrue(batch.isSelected(i));
    }

    // Deselect some entries
    batch.deselect(2);
    batch.deselect(7);
    assertFalse(batch.isSelected(2));
    assertFalse(batch.isSelected(7));
    assertTrue(batch.isSelected(3));
    assertEquals(8, batch.getNumSelected());

    // Apply a mask that clears entries 0 and 1
    long[] mask = new long[] {~0L & ~(1L << 0) & ~(1L << 1)};
    batch.andSelectionMask(mask);
    assertFalse(batch.isSelected(0));
    assertFalse(batch.isSelected(1));
    assertTrue(batch.isSelected(3));
    assertEquals(6, batch.getNumSelected());
  }

  @Test
  public void testFilterWithKeyRefBatch() throws IOException {
    // Build a KeyRefBatch with mixed visibilities
    Key[] keys = new Key[6];
    Value[] values = new Value[6];
    String[] vis = {"A", "B", "A", "B", "A", "B"};
    for (int i = 0; i < 6; i++) {
      keys[i] = new Key("row" + i, "cf", "cq", vis[i], 1000L);
      values[i] = new Value("v".getBytes(UTF_8));
    }
    KeyRefBatch batch = new KeyRefBatch(keys, values, 6);

    // Create filter with authorizations for "A" only
    VisibilityColumnarFilter filter = new VisibilityColumnarFilter();
    filter.init(Collections.emptyMap(), new ClientIteratorEnvironment.Builder()
        .withScope(IteratorScope.scan).withAuthorizations(new Authorizations("A")).build());

    ColumnarFilterContext ctx = new ColumnarFilterContext();
    ctx.setBatch(batch);
    filter.filter(ctx);

    // Only entries with visibility "A" should remain selected
    assertTrue(batch.isSelected(0));
    assertFalse(batch.isSelected(1));
    assertTrue(batch.isSelected(2));
    assertFalse(batch.isSelected(3));
    assertTrue(batch.isSelected(4));
    assertFalse(batch.isSelected(5));
    assertEquals(3, batch.getNumSelected());
  }

  @Test
  public void testGetLastKey() {
    Key[] keys = new Key[3];
    Value[] values = new Value[3];
    for (int i = 0; i < 3; i++) {
      keys[i] = new Key("row" + i, "cf", "cq", "", 1000L);
      values[i] = new Value("v".getBytes(UTF_8));
    }
    KeyRefBatch batch = new KeyRefBatch(keys, values, 3);

    assertSame(keys[2], batch.getLastKey());
  }

  @Test
  public void testEmptyBatch() {
    KeyRefBatch batch = new KeyRefBatch(new Key[0], new Value[0], 0);

    assertEquals(0, batch.getNumEntries());
    assertEquals(0, batch.getNumSelected());
    assertEquals(0, batch.getVisDictSize());
    assertNull(batch.getLastKey());
  }
}
