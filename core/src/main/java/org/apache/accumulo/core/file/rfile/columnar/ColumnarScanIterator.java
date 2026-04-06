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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;

/**
 * A per-scan wrapper that applies columnar batch filtering on top of a standard file reader. This
 * iterator reads entries from its source, builds {@link KeyValueBatch} objects, applies
 * {@link ColumnarBatchFilter} instances (visibility, column family, etc.), and serves selected
 * entries one at a time.
 *
 * <p>
 * This wrapper is created per-scan (not shared across scans) so it can hold scan-specific state
 * like authorizations and column families.
 *
 * <p>
 * The batch threshold controls batch sizing: below the threshold, single-entry batches are built.
 * At/above the threshold, larger batches are built for scan-heavy workloads.
 *
 * @since 2.1.5
 */
public class ColumnarScanIterator implements InterruptibleIterator {

  private static final int MAX_BATCH_SIZE = 10_000;

  private final SortedKeyValueIterator<Key,Value> source;
  private final List<ColumnarBatchFilter> filters;
  private final int batchThreshold;
  private final ColumnarFilterContext filterCtx = new ColumnarFilterContext();

  private ColumnarBatchRowIterator batchIter;
  private int consecutiveNextCount;

  public ColumnarScanIterator(SortedKeyValueIterator<Key,Value> source,
      List<ColumnarBatchFilter> filters, int batchThreshold) {
    this.source = source;
    this.filters = filters;
    this.batchThreshold = batchThreshold;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    filterCtx.setColumnFamilies(columnFamilies, inclusive);

    source.seek(range, columnFamilies, inclusive);
    batchIter = null;
    consecutiveNextCount = 0;

    if (source.hasTop()) {
      buildBatch();
    }
  }

  @Override
  public void next() throws IOException {
    if (batchIter != null && batchIter.hasNext()) {
      batchIter.advance();
      consecutiveNextCount++;
      return;
    }

    // Batch exhausted — build next batch (source is already positioned at the next unread entry)
    batchIter = null;

    if (source.hasTop()) {
      consecutiveNextCount++;
      buildBatch();
    }
  }

  private void buildBatch() throws IOException {
    while (source.hasTop()) {
      // Determine batch size based on threshold
      int batchSize = consecutiveNextCount < batchThreshold ? 1 : MAX_BATCH_SIZE;

      // readFromIterator advances the source past all consumed entries
      ColumnarBatch batch = KeyValueBatchBuilder.readFromIterator(source, batchSize);

      // Apply all columnar filters
      filterCtx.setBatch(batch);
      for (ColumnarBatchFilter filter : filters) {
        filter.filter(filterCtx);
      }

      batchIter = new ColumnarBatchRowIterator(batch);

      // If all entries in this batch were filtered, try the next batch
      if (!batchIter.hasNext()) {
        batchIter = null;
        continue;
      }

      // Position at the first selected entry
      batchIter.advance();
      return;
    }
  }

  @Override
  public boolean hasTop() {
    return batchIter != null;
  }

  @Override
  public Key getTopKey() {
    return batchIter.currentKey();
  }

  @Override
  public Value getTopValue() {
    return batchIter.currentValue();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    if (source instanceof InterruptibleIterator) {
      ((InterruptibleIterator) source).setInterruptFlag(flag);
    }
  }
}
