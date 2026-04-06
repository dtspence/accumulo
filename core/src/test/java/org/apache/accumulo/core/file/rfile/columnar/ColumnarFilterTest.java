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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iteratorsImpl.ClientIteratorEnvironment;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;

public class ColumnarFilterTest {

  private KeyValueBatch createBatchWithCFs(String... cfs) {
    int n = cfs.length;
    byte[][] rows = new byte[n][];
    byte[][] cfArr = new byte[n][];
    byte[][] cqs = new byte[n][];
    byte[][] cvs = new byte[n][];
    long[] ts = new long[n];
    boolean[] del = new boolean[n];
    byte[][] vals = new byte[n][];
    for (int i = 0; i < n; i++) {
      rows[i] = ("row" + i).getBytes(UTF_8);
      cfArr[i] = cfs[i].getBytes(UTF_8);
      cqs[i] = "cq".getBytes(UTF_8);
      cvs[i] = new byte[0];
      ts[i] = 1000 - i;
      del[i] = false;
      vals[i] = ("v" + i).getBytes(UTF_8);
    }
    return new KeyValueBatch(rows, cfArr, cqs, cvs, ts, del, vals, n);
  }

  private KeyValueBatch createBatchWithVisibilities(String... visibilities) {
    int n = visibilities.length;
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
      cvs[i] = visibilities[i].getBytes(UTF_8);
      ts[i] = 1000 - i;
      del[i] = false;
      vals[i] = ("v" + i).getBytes(UTF_8);
    }
    return new KeyValueBatch(rows, cfs, cqs, cvs, ts, del, vals, n);
  }

  private KeyValueBatch createBatchWithDeletes(boolean... deletes) {
    int n = deletes.length;
    byte[][] rows = new byte[n][];
    byte[][] cfs = new byte[n][];
    byte[][] cqs = new byte[n][];
    byte[][] cvs = new byte[n][];
    long[] ts = new long[n];
    byte[][] vals = new byte[n][];
    for (int i = 0; i < n; i++) {
      rows[i] = "row".getBytes(UTF_8);
      cfs[i] = "cf".getBytes(UTF_8);
      cqs[i] = "cq".getBytes(UTF_8);
      cvs[i] = new byte[0];
      ts[i] = 1000 - i; // decreasing timestamps (sorted order)
      vals[i] = ("v" + i).getBytes(UTF_8);
    }
    return new KeyValueBatch(rows, cfs, cqs, cvs, ts, deletes, vals, n);
  }

  // --- ColumnFamilyColumnarFilter tests ---

  private ColumnarFilterContext ctxFor(KeyValueBatch batch) {
    ColumnarFilterContext ctx = new ColumnarFilterContext();
    ctx.setBatch(batch);
    return ctx;
  }

  @Test
  public void testCFFilterInclusive() throws IOException {
    KeyValueBatch batch = createBatchWithCFs("a", "b", "c", "a", "d");

    ColumnFamilyColumnarFilter filter = new ColumnFamilyColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    ColumnarFilterContext ctx = ctxFor(batch);
    Set<ByteSequence> wanted = Set.of(new ArrayByteSequence("a".getBytes(UTF_8)),
        new ArrayByteSequence("c".getBytes(UTF_8)));
    ctx.setColumnFamilies(wanted, true);

    filter.filter(ctx);

    assertTrue(batch.isSelected(0)); // a - wanted
    assertFalse(batch.isSelected(1)); // b - not wanted
    assertTrue(batch.isSelected(2)); // c - wanted
    assertTrue(batch.isSelected(3)); // a - wanted
    assertFalse(batch.isSelected(4)); // d - not wanted
    assertEquals(3, batch.getNumSelected());
  }

  @Test
  public void testCFFilterExclusive() throws IOException {
    KeyValueBatch batch = createBatchWithCFs("a", "b", "c", "a", "d");

    ColumnFamilyColumnarFilter filter = new ColumnFamilyColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    ColumnarFilterContext ctx = ctxFor(batch);
    Set<ByteSequence> excluded = Set.of(new ArrayByteSequence("b".getBytes(UTF_8)));
    ctx.setColumnFamilies(excluded, false);

    filter.filter(ctx);

    assertTrue(batch.isSelected(0)); // a - not excluded
    assertFalse(batch.isSelected(1)); // b - excluded
    assertTrue(batch.isSelected(2)); // c - not excluded
    assertTrue(batch.isSelected(3)); // a - not excluded
    assertTrue(batch.isSelected(4)); // d - not excluded
    assertEquals(4, batch.getNumSelected());
  }

  @Test
  public void testCFFilterInclusiveEmptySet() throws IOException {
    KeyValueBatch batch = createBatchWithCFs("a", "b");

    ColumnFamilyColumnarFilter filter = new ColumnFamilyColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    ColumnarFilterContext ctx = ctxFor(batch);
    ctx.setColumnFamilies(Collections.emptySet(), true);

    filter.filter(ctx);

    // Inclusive with empty set = nothing matches
    assertEquals(0, batch.getNumSelected());
  }

  @Test
  public void testCFFilterNoCFs() throws IOException {
    KeyValueBatch batch = createBatchWithCFs("a", "b");

    ColumnFamilyColumnarFilter filter = new ColumnFamilyColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    ColumnarFilterContext ctx = ctxFor(batch);

    filter.filter(ctx);

    // No filter configured - keep all
    assertEquals(2, batch.getNumSelected());
  }

  // --- DeleteColumnarFilter tests ---

  @Test
  public void testDeleteFilter() throws IOException {
    // Entries: row/cf/cq with timestamps 1000, 999, 998
    // Entry 0 is a delete marker
    KeyValueBatch batch = createBatchWithDeletes(true, false, false);

    DeleteColumnarFilter filter = new DeleteColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    filter.filter(ctxFor(batch));

    assertFalse(batch.isSelected(0)); // delete marker itself
    assertFalse(batch.isSelected(1)); // same row/cf/cq/cv - deleted
    assertFalse(batch.isSelected(2)); // same row/cf/cq/cv - deleted
  }

  @Test
  public void testDeleteFilterDifferentKeys() throws IOException {
    // Create entries with different rows - delete should only affect matching entries
    int n = 4;
    byte[][] rows = new byte[n][];
    byte[][] cfs = new byte[n][];
    byte[][] cqs = new byte[n][];
    byte[][] cvs = new byte[n][];
    long[] ts = new long[n];
    boolean[] del = {true, false, false, false};
    byte[][] vals = new byte[n][];

    rows[0] = "rowA".getBytes(UTF_8); // delete marker
    rows[1] = "rowA".getBytes(UTF_8); // same key - should be deleted
    rows[2] = "rowB".getBytes(UTF_8); // different row - should NOT be deleted
    rows[3] = "rowB".getBytes(UTF_8);

    for (int i = 0; i < n; i++) {
      cfs[i] = "cf".getBytes(UTF_8);
      cqs[i] = "cq".getBytes(UTF_8);
      cvs[i] = new byte[0];
      ts[i] = 1000 - i;
      vals[i] = ("v" + i).getBytes(UTF_8);
    }

    KeyValueBatch batch = new KeyValueBatch(rows, cfs, cqs, cvs, ts, del, vals, n);

    DeleteColumnarFilter filter = new DeleteColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    filter.filter(ctxFor(batch));

    assertFalse(batch.isSelected(0)); // delete marker
    assertFalse(batch.isSelected(1)); // rowA deleted
    assertTrue(batch.isSelected(2)); // rowB not affected
    assertTrue(batch.isSelected(3)); // rowB not affected
  }

  @Test
  public void testDeleteFilterNoDeletes() throws IOException {
    KeyValueBatch batch = createBatchWithDeletes(false, false, false);

    DeleteColumnarFilter filter = new DeleteColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    filter.filter(ctxFor(batch));

    assertEquals(3, batch.getNumSelected());
  }

  // --- VisibilityColumnarFilter tests ---

  private ClientIteratorEnvironment envWithAuths(String... auths) {
    return new ClientIteratorEnvironment.Builder().withScope(IteratorScope.scan)
        .withAuthorizations(new Authorizations(auths)).build();
  }

  @Test
  public void testVisibilityFilterAllowed() throws IOException {
    KeyValueBatch batch = createBatchWithVisibilities("A", "B", "A&B", "");

    VisibilityColumnarFilter filter = new VisibilityColumnarFilter();
    filter.init(Collections.emptyMap(), envWithAuths("A", "B"));
    filter.filter(ctxFor(batch));

    assertTrue(batch.isSelected(0)); // A - user has A
    assertTrue(batch.isSelected(1)); // B - user has B
    assertTrue(batch.isSelected(2)); // A&B - user has both
    assertTrue(batch.isSelected(3)); // empty - always passes
    assertEquals(4, batch.getNumSelected());
  }

  @Test
  public void testVisibilityFilterPartialAuth() throws IOException {
    KeyValueBatch batch = createBatchWithVisibilities("A", "B", "A&B", "C");

    VisibilityColumnarFilter filter = new VisibilityColumnarFilter();
    filter.init(Collections.emptyMap(), envWithAuths("A"));
    filter.filter(ctxFor(batch));

    assertTrue(batch.isSelected(0)); // A - allowed
    assertFalse(batch.isSelected(1)); // B - not allowed
    assertFalse(batch.isSelected(2)); // A&B - not allowed (missing B)
    assertFalse(batch.isSelected(3)); // C - not allowed
    assertEquals(1, batch.getNumSelected());
  }

  @Test
  public void testVisibilityFilterEmptyAuths() throws IOException {
    KeyValueBatch batch = createBatchWithVisibilities("A", "", "B");

    VisibilityColumnarFilter filter = new VisibilityColumnarFilter();
    filter.init(Collections.emptyMap(), envWithAuths());
    filter.filter(ctxFor(batch));

    assertFalse(batch.isSelected(0)); // A - not allowed with empty auths
    assertTrue(batch.isSelected(1)); // empty vis - always passes
    assertFalse(batch.isSelected(2)); // B - not allowed
    assertEquals(1, batch.getNumSelected());
  }

  @Test
  public void testVisibilityFilterNullEnv() throws IOException {
    KeyValueBatch batch = createBatchWithVisibilities("A", "B");

    VisibilityColumnarFilter filter = new VisibilityColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    filter.filter(ctxFor(batch));

    // Null env means no-op; all entries remain selected
    assertEquals(2, batch.getNumSelected());
  }

  @Test
  public void testVisibilityFilterOrExpression() throws IOException {
    KeyValueBatch batch = createBatchWithVisibilities("A|B", "C|D", "A&C");

    VisibilityColumnarFilter filter = new VisibilityColumnarFilter();
    filter.init(Collections.emptyMap(), envWithAuths("A"));
    filter.filter(ctxFor(batch));

    assertTrue(batch.isSelected(0)); // A|B - user has A
    assertFalse(batch.isSelected(1)); // C|D - user has neither
    assertFalse(batch.isSelected(2)); // A&C - missing C
    assertEquals(1, batch.getNumSelected());
  }

  @Test
  public void testVisibilityFilterManyDuplicates() throws IOException {
    // 100 entries with only 3 distinct visibilities — exercises dictionary dedup
    String[] choices = {"A", "B", "C"};
    String[] vises = new String[100];
    for (int i = 0; i < 100; i++) {
      vises[i] = choices[i % 3];
    }
    KeyValueBatch batch = createBatchWithVisibilities(vises);

    // Verify dictionary was built correctly
    assertEquals(3, batch.getVisDictSize());

    VisibilityColumnarFilter filter = new VisibilityColumnarFilter();
    filter.init(Collections.emptyMap(), envWithAuths("A", "C"));
    filter.filter(ctxFor(batch));

    // A and C pass, B does not
    for (int i = 0; i < 100; i++) {
      if (i % 3 == 1) {
        assertFalse(batch.isSelected(i), "entry " + i + " (B) should be deselected");
      } else {
        assertTrue(batch.isSelected(i), "entry " + i + " should be selected");
      }
    }
  }

  @Test
  public void testVisibilityFilterPreDeselected() throws IOException {
    KeyValueBatch batch = createBatchWithVisibilities("A", "B", "C", "A");

    // Pre-deselect entries 0 and 2 (simulating a prior filter)
    batch.deselect(0);
    batch.deselect(2);

    VisibilityColumnarFilter filter = new VisibilityColumnarFilter();
    filter.init(Collections.emptyMap(), envWithAuths("A"));
    filter.filter(ctxFor(batch));

    assertFalse(batch.isSelected(0)); // pre-deselected
    assertFalse(batch.isSelected(1)); // B - not allowed
    assertFalse(batch.isSelected(2)); // pre-deselected
    assertTrue(batch.isSelected(3)); // A - allowed
    assertEquals(1, batch.getNumSelected());
  }

  @Test
  public void testVisibilityFilterAppliesMaskViaContext() throws IOException {
    KeyValueBatch batch = createBatchWithVisibilities("A", "B", "C");

    VisibilityColumnarFilter filter = new VisibilityColumnarFilter();
    filter.init(Collections.emptyMap(), envWithAuths("A"));
    filter.filter(ctxFor(batch));

    assertTrue(batch.isSelected(0)); // A passes
    assertFalse(batch.isSelected(1)); // B fails
    assertFalse(batch.isSelected(2)); // C fails
    assertEquals(1, batch.getNumSelected());
  }

  // --- Chained filter tests ---

  @Test
  public void testChainedCFThenVisibility() throws IOException {
    // 5 entries: different CFs and visibilities
    int n = 5;
    byte[][] rows = new byte[n][];
    byte[][] cfs = new byte[n][];
    byte[][] cqs = new byte[n][];
    byte[][] cvs = new byte[n][];
    long[] ts = new long[n];
    boolean[] del = new boolean[n];
    byte[][] vals = new byte[n][];

    String[] cfVals = {"a", "b", "a", "a", "b"};
    String[] visVals = {"X", "X", "Y", "X", "Y"};

    for (int i = 0; i < n; i++) {
      rows[i] = ("row" + i).getBytes(UTF_8);
      cfs[i] = cfVals[i].getBytes(UTF_8);
      cqs[i] = "cq".getBytes(UTF_8);
      cvs[i] = visVals[i].getBytes(UTF_8);
      ts[i] = 1000 - i;
      del[i] = false;
      vals[i] = ("v" + i).getBytes(UTF_8);
    }
    KeyValueBatch batch = new KeyValueBatch(rows, cfs, cqs, cvs, ts, del, vals, n);

    // CF filter: keep only cf=a (inclusive)
    ColumnFamilyColumnarFilter cfFilter = new ColumnFamilyColumnarFilter();
    cfFilter.init(Collections.emptyMap(), null);
    ColumnarFilterContext ctx = ctxFor(batch);
    ctx.setColumnFamilies(Set.of(new ArrayByteSequence("a".getBytes(UTF_8))), true);
    cfFilter.filter(ctx);

    // After CF filter: entries 0,2,3 selected (cf=a), entries 1,4 deselected (cf=b)
    assertTrue(batch.isSelected(0));
    assertFalse(batch.isSelected(1));
    assertTrue(batch.isSelected(2));
    assertTrue(batch.isSelected(3));
    assertFalse(batch.isSelected(4));

    // Visibility filter: only auth X
    VisibilityColumnarFilter visFilter = new VisibilityColumnarFilter();
    visFilter.init(Collections.emptyMap(), envWithAuths("X"));
    visFilter.filter(ctx);

    // After vis filter: entry 0 (a,X) and entry 3 (a,X) survive; entry 2 (a,Y) deselected
    assertTrue(batch.isSelected(0));
    assertFalse(batch.isSelected(1));
    assertFalse(batch.isSelected(2));
    assertTrue(batch.isSelected(3));
    assertFalse(batch.isSelected(4));
    assertEquals(2, batch.getNumSelected());
  }

  @Test
  public void testChainedDeleteThenCFThenVisibility() throws IOException {
    int n = 6;
    byte[][] rows = new byte[n][];
    byte[][] cfs = new byte[n][];
    byte[][] cqs = new byte[n][];
    byte[][] cvs = new byte[n][];
    long[] ts = new long[n];
    boolean[] del = new boolean[n];
    byte[][] vals = new byte[n][];

    // Entry 0: delete marker for rowA/a/cq/X
    // Entry 1: rowA/a/cq/X (deleted by entry 0)
    // Entry 2: rowB/a/cq/X (not deleted, passes CF and vis)
    // Entry 3: rowC/b/cq/X (not deleted, fails CF filter)
    // Entry 4: rowD/a/cq/Y (not deleted, passes CF, fails vis)
    // Entry 5: rowE/a/cq/X (not deleted, passes all)
    String[] rowVals = {"rowA", "rowA", "rowB", "rowC", "rowD", "rowE"};
    String[] cfVals = {"a", "a", "a", "b", "a", "a"};
    String[] visVals = {"X", "X", "X", "X", "Y", "X"};
    boolean[] delVals = {true, false, false, false, false, false};

    for (int i = 0; i < n; i++) {
      rows[i] = rowVals[i].getBytes(UTF_8);
      cfs[i] = cfVals[i].getBytes(UTF_8);
      cqs[i] = "cq".getBytes(UTF_8);
      cvs[i] = visVals[i].getBytes(UTF_8);
      ts[i] = 1000 - i;
      del[i] = delVals[i];
      vals[i] = ("v" + i).getBytes(UTF_8);
    }
    KeyValueBatch batch = new KeyValueBatch(rows, cfs, cqs, cvs, ts, del, vals, n);
    ColumnarFilterContext ctx = ctxFor(batch);

    // Apply delete filter
    DeleteColumnarFilter delFilter = new DeleteColumnarFilter();
    delFilter.init(Collections.emptyMap(), null);
    delFilter.filter(ctx);

    // Apply CF filter: keep cf=a
    ctx.setColumnFamilies(Set.of(new ArrayByteSequence("a".getBytes(UTF_8))), true);
    ColumnFamilyColumnarFilter cfFilter = new ColumnFamilyColumnarFilter();
    cfFilter.init(Collections.emptyMap(), null);
    cfFilter.filter(ctx);

    // Apply visibility filter: auth X
    VisibilityColumnarFilter visFilter = new VisibilityColumnarFilter();
    visFilter.init(Collections.emptyMap(), envWithAuths("X"));
    visFilter.filter(ctx);

    // Only entries 2 (rowB/a/cq/X) and 5 (rowE/a/cq/X) should survive
    assertFalse(batch.isSelected(0)); // delete marker
    assertFalse(batch.isSelected(1)); // deleted
    assertTrue(batch.isSelected(2)); // passes all
    assertFalse(batch.isSelected(3)); // cf=b excluded
    assertFalse(batch.isSelected(4)); // vis=Y excluded
    assertTrue(batch.isSelected(5)); // passes all
    assertEquals(2, batch.getNumSelected());
  }

  // --- Empty batch filter tests ---

  @Test
  public void testCFFilterEmptyBatch() throws IOException {
    KeyValueBatch batch = createBatchWithCFs();
    ColumnFamilyColumnarFilter filter = new ColumnFamilyColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    ColumnarFilterContext ctx = ctxFor(batch);
    ctx.setColumnFamilies(Set.of(new ArrayByteSequence("a".getBytes(UTF_8))), true);
    filter.filter(ctx);
    assertEquals(0, batch.getNumSelected());
  }

  @Test
  public void testDeleteFilterEmptyBatch() throws IOException {
    KeyValueBatch batch = createBatchWithDeletes();
    DeleteColumnarFilter filter = new DeleteColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    filter.filter(ctxFor(batch));
    assertEquals(0, batch.getNumSelected());
  }

  @Test
  public void testVisibilityFilterEmptyBatch() throws IOException {
    KeyValueBatch batch = createBatchWithVisibilities();
    VisibilityColumnarFilter filter = new VisibilityColumnarFilter();
    filter.init(Collections.emptyMap(), envWithAuths("A"));
    filter.filter(ctxFor(batch));
    assertEquals(0, batch.getNumSelected());
  }

  // --- Word-boundary filter tests (64, 65 entries) ---

  @Test
  public void testCFFilterExact64Entries() throws IOException {
    // 64 entries = exactly 1 bitmask word
    String[] cfs = IntStream.range(0, 64).mapToObj(i -> i % 3 == 0 ? "wanted" : "other")
        .toArray(String[]::new);
    KeyValueBatch batch = createBatchWithCFs(cfs);

    ColumnFamilyColumnarFilter filter = new ColumnFamilyColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    ColumnarFilterContext ctx = ctxFor(batch);
    ctx.setColumnFamilies(Set.of(new ArrayByteSequence("wanted".getBytes(UTF_8))), true);
    filter.filter(ctx);

    int expectedCount = 0;
    for (int i = 0; i < 64; i++) {
      if (i % 3 == 0) {
        assertTrue(batch.isSelected(i), "entry " + i + " (wanted) should be selected");
        expectedCount++;
      } else {
        assertFalse(batch.isSelected(i), "entry " + i + " (other) should be deselected");
      }
    }
    assertEquals(expectedCount, batch.getNumSelected());
  }

  @Test
  public void testCFFilterExact65Entries() throws IOException {
    // 65 entries = crosses word boundary (2 words, second has 1 bit)
    String[] cfs =
        IntStream.range(0, 65).mapToObj(i -> i % 2 == 0 ? "keep" : "drop").toArray(String[]::new);
    KeyValueBatch batch = createBatchWithCFs(cfs);

    ColumnFamilyColumnarFilter filter = new ColumnFamilyColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    ColumnarFilterContext ctx = ctxFor(batch);
    ctx.setColumnFamilies(Set.of(new ArrayByteSequence("keep".getBytes(UTF_8))), true);
    filter.filter(ctx);

    for (int i = 0; i < 65; i++) {
      if (i % 2 == 0) {
        assertTrue(batch.isSelected(i), "entry " + i + " (keep) should be selected");
      } else {
        assertFalse(batch.isSelected(i), "entry " + i + " (drop) should be deselected");
      }
    }
    assertEquals(33, batch.getNumSelected());
  }

  @Test
  public void testCFFilterExclusiveAt64Boundary() throws IOException {
    // Exclusive mode at word boundary — verify trailing bits cleared correctly
    String[] cfs =
        IntStream.range(0, 64).mapToObj(i -> i < 5 ? "excluded" : "kept").toArray(String[]::new);
    KeyValueBatch batch = createBatchWithCFs(cfs);

    ColumnFamilyColumnarFilter filter = new ColumnFamilyColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    ColumnarFilterContext ctx = ctxFor(batch);
    ctx.setColumnFamilies(Set.of(new ArrayByteSequence("excluded".getBytes(UTF_8))), false);
    filter.filter(ctx);

    for (int i = 0; i < 64; i++) {
      if (i < 5) {
        assertFalse(batch.isSelected(i), "entry " + i + " should be deselected (excluded)");
      } else {
        assertTrue(batch.isSelected(i), "entry " + i + " should be selected (kept)");
      }
    }
    assertEquals(59, batch.getNumSelected());
  }

  @Test
  public void testVisibilityFilter128Entries() throws IOException {
    // 128 entries = exactly 2 full bitmask words
    String[] vises =
        IntStream.range(0, 128).mapToObj(i -> i % 4 == 0 ? "A" : "B").toArray(String[]::new);
    KeyValueBatch batch = createBatchWithVisibilities(vises);

    VisibilityColumnarFilter filter = new VisibilityColumnarFilter();
    filter.init(Collections.emptyMap(), envWithAuths("A"));
    filter.filter(ctxFor(batch));

    for (int i = 0; i < 128; i++) {
      if (i % 4 == 0) {
        assertTrue(batch.isSelected(i), "entry " + i + " (A) should be selected");
      } else {
        assertFalse(batch.isSelected(i), "entry " + i + " (B) should be deselected");
      }
    }
    assertEquals(32, batch.getNumSelected());
  }

  // --- Delete filter edge cases ---

  @Test
  public void testDeleteMarkerAtEndOfBatch() throws IOException {
    // Delete marker is the last entry — nothing after it to delete
    KeyValueBatch batch = createBatchWithDeletes(false, false, true);
    DeleteColumnarFilter filter = new DeleteColumnarFilter();
    filter.init(Collections.emptyMap(), null);
    filter.filter(ctxFor(batch));

    assertTrue(batch.isSelected(0)); // not deleted
    assertTrue(batch.isSelected(1)); // not deleted
    assertFalse(batch.isSelected(2)); // delete marker deselected
    assertEquals(2, batch.getNumSelected());
  }
}
