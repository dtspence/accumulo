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
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

/**
 * Deselects entries whose column family is not in the fetch set. This is valuable for the default
 * locality group, which contains all CFs not in explicit locality groups. Without this filter,
 * every unwanted-CF entry in a block would be materialized as Key/Value and passed up to
 * ColumnFamilySkippingIterator for expensive skip/re-seek handling.
 *
 * <p>
 * Uses a matrix-vector approach: builds an M x numWords bitmask matrix (one row per fetch CF) in a
 * single pass, then OR-reduces matching rows into a result mask applied via word-level AND. The
 * comparison phase does O(N * F/2) {@link Arrays#equals} calls (F = fetch set size), and the
 * reduction phase is pure word-level OR — trivially SIMD-vectorizable.
 *
 * <p>
 * Column families and inclusiveness are read from the {@link ColumnarFilterContext} at filter time.
 *
 * @since 2.1.5
 */
public class ColumnFamilyColumnarFilter implements ColumnarBatchFilter {

  @Override
  public void init(Map<String,String> options, IteratorEnvironment env) {
    // Column families come from ColumnarFilterContext, not from options
  }

  @Override
  public void filter(ColumnarFilterContext ctx) {
    ColumnarBatch batch = ctx.getBatch();
    Collection<ByteSequence> columnFamilies = ctx.getColumnFamilies();
    boolean inclusive = ctx.isColumnFamilyInclusive();
    int numEntries = batch.getNumEntries();
    int numWords = batch.getSelectionMaskLength();

    Set<ByteSequence> colFamSet;
    if (columnFamilies instanceof Set) {
      colFamSet = (Set<ByteSequence>) columnFamilies;
    } else {
      colFamSet = new HashSet<>(columnFamilies);
    }

    if (colFamSet.isEmpty()) {
      // Inclusive with empty set = nothing matches, deselect all;
      // not inclusive with empty set = keep all (no exclusion filter)
      if (inclusive) {
        ctx.applyMask(new long[numWords]);
      }
      return;
    }

    long[] mask = computeFilterMask(batch, colFamSet, inclusive, numEntries, numWords);
    ctx.applyMask(mask);
  }

  private long[] computeFilterMask(ColumnarBatch batch, Set<ByteSequence> colFamSet,
      boolean inclusive, int numEntries, int numWords) {

    // Convert fetch CFs to byte[][] for direct Arrays.equals comparison
    byte[][] fetchCfBytes = new byte[colFamSet.size()][];
    int fi = 0;
    for (ByteSequence bs : colFamSet) {
      fetchCfBytes[fi++] = bs.toArray();
    }

    // Build matrix: one row per fetch CF, columns are bitmask words.
    // Single pass through entries; each entry compared against fetch CFs.
    long[][] cfMatrix = new long[fetchCfBytes.length][numWords];
    for (int i = 0; i < numEntries; i++) {
      byte[] cf = batch.getColFamily(i);
      for (int f = 0; f < fetchCfBytes.length; f++) {
        if (Arrays.equals(cf, fetchCfBytes[f])) {
          cfMatrix[f][i >>> 6] |= (1L << (i & 63));
          break;
        }
      }
    }

    // Reduce: OR all rows into result mask
    long[] mask = new long[numWords];
    for (int f = 0; f < fetchCfBytes.length; f++) {
      for (int w = 0; w < numWords; w++) {
        mask[w] |= cfMatrix[f][w];
      }
    }

    // Exclusive: invert the mask (keep entries NOT matching any excluded CF)
    if (!inclusive) {
      for (int w = 0; w < numWords; w++) {
        mask[w] = ~mask[w];
      }
      clearTrailingBits(mask, numWords, numEntries);
    }

    return mask;
  }

  private static void clearTrailingBits(long[] mask, int numWords, int numEntries) {
    int trailing = numEntries & 63;
    if (trailing != 0 && numWords > 0) {
      mask[numWords - 1] &= (1L << trailing) - 1;
    }
  }
}
