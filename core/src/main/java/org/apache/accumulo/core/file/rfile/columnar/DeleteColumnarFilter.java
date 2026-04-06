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
import java.util.Map;

import org.apache.accumulo.core.iterators.IteratorEnvironment;

/**
 * Handles delete markers in the columnar batch for scan scope. When a delete marker is found,
 * deselects the marker itself and all subsequent entries with the same row/cf/cq/cv (which the
 * delete logically removes).
 *
 * <p>
 * This filter is ONLY safe for scan-time use, not for compaction scope where delete markers must be
 * propagated through the iterator stack.
 *
 * <p>
 * Entries within a block are sorted, so entries sharing row/cf/cq/cv are consecutive. When a delete
 * marker is encountered, we walk forward deselecting entries that match until the key fields
 * diverge.
 *
 * @since 2.1.5
 */
public class DeleteColumnarFilter implements ColumnarBatchFilter {

  @Override
  public void init(Map<String,String> options, IteratorEnvironment env) {
    // No configuration needed
  }

  @Override
  public void filter(ColumnarFilterContext ctx) {
    ColumnarBatch batch = ctx.getBatch();
    int numEntries = batch.getNumEntries();

    for (int i = 0; i < numEntries; i++) {
      if (!batch.isSelected(i)) {
        continue;
      }

      if (!batch.isDeleted(i)) {
        continue;
      }

      // Found a delete marker — deselect it
      ctx.deselect(i);

      // Deselect all subsequent entries with the same row/cf/cq/cv
      // (these are the entries being logically deleted)
      byte[] delRow = batch.getRow(i);
      byte[] delCf = batch.getColFamily(i);
      byte[] delCq = batch.getColQualifier(i);
      byte[] delCv = batch.getColVisibility(i);

      int j = i + 1;
      for (; j < numEntries; j++) {
        // Since entries are sorted, once the row/cf/cq/cv changes, we're done
        if (!Arrays.equals(batch.getRow(j), delRow) || !Arrays.equals(batch.getColFamily(j), delCf)
            || !Arrays.equals(batch.getColQualifier(j), delCq)
            || !Arrays.equals(batch.getColVisibility(j), delCv)) {
          break;
        }

        ctx.deselect(j);
      }
      // Skip past the entries we already processed
      i = j - 1;
    }
  }
}
