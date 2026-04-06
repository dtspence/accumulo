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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates column visibility expressions against scan authorizations and deselects entries the
 * user is not authorized to see. Uses the batch's visibility dictionary to evaluate each unique
 * visibility exactly once per batch, then produces a bitmask for word-level AND with the selection
 * vector.
 *
 * <p>
 * Evaluation results are cached across batches so that each unique visibility expression is parsed
 * and evaluated at most once per scan.
 *
 * @since 2.1.5
 */
public class VisibilityColumnarFilter implements ColumnarBatchFilter {

  private static final Logger log = LoggerFactory.getLogger(VisibilityColumnarFilter.class);

  private VisibilityEvaluator ve;
  private Authorizations authorizations;
  private boolean emptyAuths;

  // Cache: visibility bytes -> evaluation result, persists across batches within a scan
  private final Map<ByteBuffer,Boolean> evalCache = new HashMap<>();

  @Override
  public void init(Map<String,String> options, IteratorEnvironment env) {
    if (env != null) {
      this.authorizations = env.getAuthorizations();
    }
    if (authorizations != null) {
      this.emptyAuths = authorizations.isEmpty();
      if (!emptyAuths) {
        this.ve = new VisibilityEvaluator(authorizations);
      }
    }
  }

  /**
   * Builds a bitmask where bit i is set if entry i passes the visibility check.
   *
   * <p>
   * The mask is computed in two phases:
   *
   * <ol>
   * <li><b>Dictionary evaluation</b> — The batch's visibility dictionary contains D unique
   * visibility expressions (typically D &lt;&lt; N). Each is evaluated once against the scan
   * authorizations, with results cached across batches so repeated expressions (common across RFile
   * blocks) are never re-parsed. This reduces O(N) ColumnVisibility parse+evaluate calls to at most
   * O(D) on the first batch and O(0) on subsequent batches with the same expressions.</li>
   *
   * <li><b>Mask construction</b> — A single pass maps each entry's dictionary ID to its pass/fail
   * result. Failing entries have their bit cleared via word-indexed shift-and-mask operations
   * ({@code mask[i >>> 6] &= ~(1L << (i & 63))}), which the JVM can vectorize.</li>
   * </ol>
   *
   * <p>
   * The resulting mask is applied to the batch's selection vector via word-level AND in
   * {@link ColumnarBatch#andSelectionMask(long[])}.
   */
  private long[] computeFilterMask(ColumnarBatch batch) {
    int numWords = batch.getSelectionMaskLength();
    int numEntries = batch.getNumEntries();
    long[] mask = new long[numWords];
    Arrays.fill(mask, ~0L);

    if (authorizations == null) {
      // No authorizations context — pass everything through
      clearTrailingBits(mask, numWords, numEntries);
      return mask;
    }

    // Phase 1: Evaluate each unique visibility in the batch's dictionary. Results are looked up
    // from evalCache (populated on prior batches) or computed and cached for future batches.
    int dictSize = batch.getVisDictSize();
    boolean[] dictResults = new boolean[dictSize];
    for (int d = 0; d < dictSize; d++) {
      byte[] vis = batch.getVisDictEntry(d);
      if (emptyAuths) {
        // User has no authorizations — only empty visibility passes
        dictResults[d] = (vis.length == 0);
      } else if (vis.length == 0) {
        // Entry has no visibility constraint — always visible to any authenticated user
        dictResults[d] = true;
      } else {
        dictResults[d] = evaluateVisibility(vis);
      }
    }

    // Phase 2: Build the pass/fail bitmask. Each entry's visDictId indexes into dictResults.
    // Entries whose visibility failed have their bit cleared in the corresponding mask word.
    for (int i = 0; i < numEntries; i++) {
      if (!dictResults[batch.getVisDictId(i)]) {
        mask[i >>> 6] &= ~(1L << (i & 63));
      }
    }

    clearTrailingBits(mask, numWords, numEntries);
    return mask;
  }

  @Override
  public void filter(ColumnarFilterContext ctx) {
    ColumnarBatch batch = ctx.getBatch();
    long[] mask = computeFilterMask(batch);
    ctx.applyMask(mask);
  }

  private boolean evaluateVisibility(byte[] visBytes) {
    ByteBuffer key = ByteBuffer.wrap(visBytes);
    Boolean cached = evalCache.get(key);
    if (cached != null) {
      return cached;
    }

    boolean result;
    try {
      result = ve.evaluate(new ColumnVisibility(visBytes));
    } catch (VisibilityParseException | BadArgumentException e) {
      log.error("Error evaluating visibility: {}", e.getMessage(), e);
      result = false;
    }
    evalCache.put(key, result);
    return result;
  }

  private static void clearTrailingBits(long[] mask, int numWords, int numEntries) {
    int trailing = numEntries & 63;
    if (trailing != 0 && numWords > 0) {
      mask[numWords - 1] &= (1L << trailing) - 1;
    }
  }
}
