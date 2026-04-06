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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import com.google.common.base.Preconditions;

/**
 * Parsing and loading utility for columnar batch iterators. Mirrors the patterns of
 * {@link org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil} but for the
 * {@code table.columnar.iterator.} prefix.
 *
 * <p>
 * Property format:
 *
 * <pre>
 * table.columnar.iterator.&lt;name&gt; = &lt;priority&gt;,&lt;className&gt;
 * table.columnar.iterator.&lt;name&gt;.opt.&lt;optionName&gt; = &lt;optionValue&gt;
 * </pre>
 *
 * @since 2.1.5
 */
public class ColumnarIteratorConfigUtil {

  private static final String PREFIX = Property.TABLE_COLUMNAR_ITERATOR_PREFIX.getKey();

  /**
   * Holds parsed info about a columnar iterator (name, priority, className).
   */
  static class ColumnarIterInfo implements Comparable<ColumnarIterInfo> {
    final String name;
    final int priority;
    final String className;

    ColumnarIterInfo(String name, int priority, String className) {
      this.name = name;
      this.priority = priority;
      this.className = className;
    }

    @Override
    public int compareTo(ColumnarIterInfo other) {
      int cmp = Integer.compare(this.priority, other.priority);
      if (cmp != 0) {
        return cmp;
      }
      return this.name.compareTo(other.name);
    }
  }

  /**
   * Parses all columnar iterator properties from the given configuration and returns instantiated,
   * initialized filters sorted by priority.
   *
   * @param conf the table configuration
   * @param env the iterator environment (provides authorizations, etc.)
   * @return list of initialized ColumnarBatchFilter instances, sorted by priority
   */
  public static List<ColumnarBatchFilter> loadColumnarFilters(AccumuloConfiguration conf,
      IteratorEnvironment env) throws IOException {

    Map<String,ColumnarIterInfo> iterInfos = new TreeMap<>();
    Map<String,Map<String,String>> iterOptions = new HashMap<>();

    // Parse properties
    Map<String,String> props = new HashMap<>();
    conf.getProperties(props, p -> p.startsWith(PREFIX));

    for (Map.Entry<String,String> entry : props.entrySet()) {
      String property = entry.getKey();
      String value = entry.getValue();

      if (!property.startsWith(PREFIX)) {
        continue;
      }

      String suffix = property.substring(PREFIX.length());
      // suffix is either "<name>" or "<name>.opt.<optionName>"
      // Split with limit 3 to handle: name, opt, optionName
      String[] parts = suffix.split("\\.", 3);

      String iterName = parts[0];
      Preconditions.checkArgument(!iterName.isEmpty(), "Empty iterator name in property: %s",
          property);

      if (parts.length == 1) {
        // Base definition: <name> = <priority>,<className>
        String[] valTokens = value.split(",", -1);
        Preconditions.checkArgument(valTokens.length == 2,
            "Expected priority,className for property: %s=%s", property, value);
        int priority = Integer.parseInt(valTokens[0].trim());
        String className = valTokens[1].trim();
        Preconditions.checkArgument(priority > 0, "Priority must be > 0 for property: %s=%s",
            property, value);
        Preconditions.checkArgument(!className.isEmpty(), "Empty class name for property: %s=%s",
            property, value);
        iterInfos.put(iterName, new ColumnarIterInfo(iterName, priority, className));
      } else if (parts.length == 3 && parts[1].equals("opt")) {
        // Option: <name>.opt.<optionName> = <value>
        String optionName = parts[2];
        Preconditions.checkArgument(!optionName.isEmpty(), "Empty option name in property: %s",
            property);
        iterOptions.computeIfAbsent(iterName, k -> new HashMap<>()).put(optionName, value);
      } else {
        throw new IllegalArgumentException(
            "Malformed columnar iterator property: " + property + "=" + value);
      }
    }

    // Sort by priority, instantiate, and initialize
    List<ColumnarIterInfo> sorted = new ArrayList<>(iterInfos.values());
    Collections.sort(sorted);

    List<ColumnarBatchFilter> filters = new ArrayList<>();
    for (ColumnarIterInfo info : sorted) {
      try {
        Class<?> clazz = Class.forName(info.className);
        ColumnarBatchFilter filter =
            (ColumnarBatchFilter) clazz.getDeclaredConstructor().newInstance();
        Map<String,String> opts = iterOptions.getOrDefault(info.name, Collections.emptyMap());
        filter.init(opts, env);
        filters.add(filter);
      } catch (ReflectiveOperationException e) {
        throw new IOException(
            "Failed to instantiate columnar iterator: " + info.name + " (" + info.className + ")",
            e);
      }
    }

    return filters;
  }
}
