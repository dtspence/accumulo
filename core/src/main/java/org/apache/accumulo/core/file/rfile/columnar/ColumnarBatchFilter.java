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
import java.util.Map;

import org.apache.accumulo.core.iterators.IteratorEnvironment;

/**
 * Interface for columnar batch filters — the columnar equivalent of
 * {@link org.apache.accumulo.core.iterators.SortedKeyValueIterator}. Implementations operate on a
 * {@link KeyValueBatch} via a {@link ColumnarFilterContext}, deselecting entries that don't pass
 * filter criteria.
 *
 * <p>
 * Filters apply results through the context: use {@link ColumnarFilterContext#deselect(int)} for
 * per-entry filtering, or {@link ColumnarFilterContext#applyMask(long[])} for word-level bitmask
 * filtering.
 *
 * <p>
 * Configured via table properties using the same semantics as regular iterators (priority, name,
 * class, options):
 *
 * <pre>
 * table.columnar.iterator.&lt;name&gt; = &lt;priority&gt;,&lt;className&gt;
 * table.columnar.iterator.&lt;name&gt;.opt.&lt;optionName&gt; = &lt;optionValue&gt;
 * </pre>
 *
 * @since 2.1.5
 */
public interface ColumnarBatchFilter {

  /**
   * Initialize this filter with configuration options and environment context. Called once when the
   * filter is instantiated.
   *
   * @param options configuration options from table properties
   * @param env iterator environment providing scan context (authorizations, scope, etc.)
   */
  void init(Map<String,String> options, IteratorEnvironment env) throws IOException;

  /**
   * Apply this filter to the current batch in the context. For each selected entry, if the entry
   * does not pass the filter, deselect it via {@link ColumnarFilterContext#deselect(int)} or apply
   * a bitmask via {@link ColumnarFilterContext#applyMask(long[])}.
   *
   * <p>
   * Implementations should only examine entries where {@link ColumnarBatch#isSelected(int)} is
   * true, to avoid redundant work on already-filtered entries.
   *
   * @param ctx the filter context carrying the batch and scan parameters
   */
  void filter(ColumnarFilterContext ctx);
}
