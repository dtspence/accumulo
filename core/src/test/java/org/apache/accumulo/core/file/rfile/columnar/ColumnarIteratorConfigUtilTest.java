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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.junit.jupiter.api.Test;

public class ColumnarIteratorConfigUtilTest {

  /**
   * A trivial ColumnarBatchFilter for testing config loading.
   */
  public static class TestFilter implements ColumnarBatchFilter {
    public boolean initCalled = false;
    public Map<String,String> initOptions;

    @Override
    public void init(Map<String,String> options, IteratorEnvironment env) {
      initCalled = true;
      initOptions = options;
    }

    @Override
    public void filter(ColumnarFilterContext ctx) {
      // no-op for config tests
    }
  }

  /**
   * Another trivial filter for testing priority ordering.
   */
  public static class TestFilterB implements ColumnarBatchFilter {
    @Override
    public void init(Map<String,String> options, IteratorEnvironment env) {}

    @Override
    public void filter(ColumnarFilterContext ctx) {}
  }

  private ConfigurationCopy configWithProps(String... keyValues) {
    ConfigurationCopy cc = new ConfigurationCopy(DefaultConfiguration.getInstance());
    for (int i = 0; i < keyValues.length; i += 2) {
      cc.set(keyValues[i], keyValues[i + 1]);
    }
    return cc;
  }

  @Test
  public void testLoadSingleFilter() throws IOException {
    ConfigurationCopy conf =
        configWithProps("table.columnar.iterator.myfilter", "10," + TestFilter.class.getName());

    List<ColumnarBatchFilter> filters = ColumnarIteratorConfigUtil.loadColumnarFilters(conf, null);

    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof TestFilter);
    assertTrue(((TestFilter) filters.get(0)).initCalled);
  }

  @Test
  public void testLoadFilterWithOptions() throws IOException {
    ConfigurationCopy conf = configWithProps("table.columnar.iterator.myfilter",
        "10," + TestFilter.class.getName(), "table.columnar.iterator.myfilter.opt.threshold", "100",
        "table.columnar.iterator.myfilter.opt.mode", "strict");

    List<ColumnarBatchFilter> filters = ColumnarIteratorConfigUtil.loadColumnarFilters(conf, null);

    assertEquals(1, filters.size());
    TestFilter filter = (TestFilter) filters.get(0);
    assertEquals("100", filter.initOptions.get("threshold"));
    assertEquals("strict", filter.initOptions.get("mode"));
  }

  @Test
  public void testPriorityOrdering() throws IOException {
    ConfigurationCopy conf =
        configWithProps("table.columnar.iterator.second", "20," + TestFilterB.class.getName(),
            "table.columnar.iterator.first", "10," + TestFilter.class.getName());

    List<ColumnarBatchFilter> filters = ColumnarIteratorConfigUtil.loadColumnarFilters(conf, null);

    assertEquals(2, filters.size());
    assertTrue(filters.get(0) instanceof TestFilter); // priority 10 first
    assertTrue(filters.get(1) instanceof TestFilterB); // priority 20 second
  }

  @Test
  public void testNoFiltersConfigured() throws IOException {
    ConfigurationCopy conf = new ConfigurationCopy(DefaultConfiguration.getInstance());

    List<ColumnarBatchFilter> filters = ColumnarIteratorConfigUtil.loadColumnarFilters(conf, null);

    assertTrue(filters.isEmpty());
  }

  @Test
  public void testInvalidClassName() {
    ConfigurationCopy conf =
        configWithProps("table.columnar.iterator.bad", "10,com.nonexistent.FakeFilter");

    assertThrows(IOException.class,
        () -> ColumnarIteratorConfigUtil.loadColumnarFilters(conf, null));
  }

  @Test
  public void testInvalidPriority() {
    ConfigurationCopy conf =
        configWithProps("table.columnar.iterator.bad", "0," + TestFilter.class.getName());

    assertThrows(IllegalArgumentException.class,
        () -> ColumnarIteratorConfigUtil.loadColumnarFilters(conf, null));
  }

  @Test
  public void testMalformedValue() {
    ConfigurationCopy conf =
        configWithProps("table.columnar.iterator.bad", "notanumber," + TestFilter.class.getName());

    assertThrows(NumberFormatException.class,
        () -> ColumnarIteratorConfigUtil.loadColumnarFilters(conf, null));
  }
}
