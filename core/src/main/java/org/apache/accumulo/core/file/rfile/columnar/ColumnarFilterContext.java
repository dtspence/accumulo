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

import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.ByteSequence;

/**
 * Mutable context passed to {@link ColumnarBatchFilter#filter(ColumnarFilterContext)} carrying
 * per-scan and per-batch state. Scan-level state (column families) is set once at seek time;
 * per-batch state is updated before each filter pass.
 *
 * <p>
 * Filters apply results through this context via {@link #deselect(int)} for per-entry filtering or
 * {@link #applyMask(long[])} for word-level bitmask filtering.
 *
 * @since 2.1.5
 */
public class ColumnarFilterContext {

  // Per-scan state (set at seek time)
  private Collection<ByteSequence> columnFamilies = Collections.emptyList();
  private boolean inclusive;

  // Per-batch state (set before each filter pass)
  private ColumnarBatch batch;

  /** Sets the column families for this scan context. Called at seek time. */
  public void setColumnFamilies(Collection<ByteSequence> columnFamilies, boolean inclusive) {
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;
  }

  /** Sets the current batch. Called before applying filters to each new batch. */
  public void setBatch(ColumnarBatch batch) {
    this.batch = batch;
  }

  /** Returns the current batch. */
  public ColumnarBatch getBatch() {
    return batch;
  }

  /** Returns the column families for this scan. */
  public Collection<ByteSequence> getColumnFamilies() {
    return columnFamilies;
  }

  /** Returns whether the column family set is inclusive or exclusive. */
  public boolean isColumnFamilyInclusive() {
    return inclusive;
  }

  /** Deselects entry {@code i} in the current batch. */
  public void deselect(int i) {
    batch.deselect(i);
  }

  /** ANDs the given pass-mask with the current batch's selection vector. */
  public void applyMask(long[] mask) {
    batch.andSelectionMask(mask);
  }
}
