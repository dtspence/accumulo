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
package org.apache.accumulo.core.client.admin.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer.InitParameters;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer.InputParameters;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.junit.jupiter.api.Test;

public class ErasureCodeConfigurerTest {

  private static class TestInitParameters implements InitParameters {
    private final Map<String,String> options;

    TestInitParameters(Map<String,String> options) {
      this.options = options;
    }

    @Override
    public Map<String,String> getOptions() {
      return options;
    }

    @Override
    public TableId getTableId() {
      return TableId.of("1");
    }

    @Override
    public PluginEnvironment getEnvironment() {
      return null; // Not needed for tests
    }
  }

  private static class TestInputParameters implements InputParameters {
    private final List<Long> fileSizes;
    private final String tableEcSetting;

    TestInputParameters(List<Long> fileSizes, String tableEcSetting) {
      this.fileSizes = fileSizes;
      this.tableEcSetting = tableEcSetting;
    }

    @Override
    public Collection<CompactableFile> getInputFiles() {
      return fileSizes.stream()
          .map(size -> CompactableFile.create(
              URI.create("hdfs://localhost:9000/accumulo/tables/1/default_tablet/F000001.rf"), size,
              100))
          .collect(Collectors.toList());
    }

    @Override
    public TableId getTableId() {
      return TableId.of("1");
    }

    @Override
    @SuppressWarnings("deprecation")
    public TabletId getTabletId() {
      return new TabletId() {
        @Override
        public int compareTo(TabletId o) {
          return 0;
        }

        @Override
        public TableId getTable() {
          return TableId.of("1");
        }

        @Override
        public org.apache.hadoop.io.Text getTableId() {
          return new org.apache.hadoop.io.Text("1");
        }

        public String canonical() {
          return "1<";
        }

        @Override
        public Range toRange() {
          return new Range();
        }

        @Override
        public org.apache.hadoop.io.Text getPrevEndRow() {
          return null;
        }

        @Override
        public org.apache.hadoop.io.Text getEndRow() {
          return null;
        }
      };
    }

    @Override
    public URI getOutputFile() {
      return URI.create("file:///tmp/test.rf");
    }

    @Override
    public PluginEnvironment getEnvironment() {
      return new PluginEnvironment() {
        @Override
        public Configuration getConfiguration(TableId tableId) {
          return new Configuration() {
            @Override
            public String get(String key) {
              if (Property.TABLE_ENABLE_ERASURE_CODES.getKey().equals(key)) {
                return tableEcSetting;
              }
              return null;
            }

            @Override
            public boolean isSet(String key) {
              return tableEcSetting != null;
            }

            @Override
            public <T> Supplier<T> getDerived(Function<Configuration,T> computeFunc) {
              return () -> computeFunc.apply(this);
            }

            @Override
            public Map<String,String> getWithPrefix(String prefix) {
              return Map.of();
            }

            @Override
            public Map<String,String> getCustom() {
              return Map.of();
            }

            @Override
            public String getCustom(String keySuffix) {
              return null;
            }

            @Override
            public Map<String,String> getTableCustom() {
              return Map.of();
            }

            @Override
            public String getTableCustom(String keySuffix) {
              return null;
            }

            @Override
            public Iterator<Entry<String,String>> iterator() {
              return Map.of(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), tableEcSetting).entrySet()
                  .iterator();
            }
          };
        }

        @Override
        public <T> T instantiate(TableId tableId, String className, Class<T> base) {
          throw new UnsupportedOperationException();
        }

        @Override
        public <T> T instantiate(String className, Class<T> base) {
          throw new UnsupportedOperationException();
        }

        @Override
        public String getTableName(TableId tableId) {
          return "testTable";
        }

        @Override
        public Configuration getConfiguration() {
          return getConfiguration(TableId.of("1"));
        }
      };
    }
  }

  private InitParameters createInitParams(Map<String,String> options) {
    return new TestInitParameters(options);
  }

  private InputParameters createInputParams(List<Long> fileSizes, String tableEcSetting) {
    return new TestInputParameters(fileSizes, tableEcSetting);
  }

  @Test
  public void testBypassErasureCodesAlwaysDisablesEC() {
    ErasureCodeConfigurer configurer = new ErasureCodeConfigurer();
    Map<String,String> options = Map.of(ErasureCodeConfigurer.BYPASS_ERASURE_CODES, "true");
    configurer.init(createInitParams(options));

    // Test with large file size and table EC enabled - should still disable
    InputParameters params = createInputParams(List.of(100_000_000L), "enable");
    var overrides = configurer.override(params);

    assertEquals("disable",
        overrides.getOverrides().get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
  }

  @Test
  public void testAboveThresholdWithDisable() {
    ErasureCodeConfigurer configurer = new ErasureCodeConfigurer();
    Map<String,String> options = Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10M");
    configurer.init(createInitParams(options));

    InputParameters params = createInputParams(List.of(11_000_000L), "disable");
    var overrides = configurer.override(params);

    assertEquals("enable",
        overrides.getOverrides().get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
  }

  @Test
  public void testAboveThresholdWithEnable() {
    ErasureCodeConfigurer configurer = new ErasureCodeConfigurer();
    Map<String,String> options = Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10M");
    configurer.init(createInitParams(options));

    InputParameters params = createInputParams(List.of(11_000_000L), "enable");
    var overrides = configurer.override(params);

    assertEquals("enable",
        overrides.getOverrides().get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
  }

  @Test
  public void testAboveThresholdWithInherit() {
    ErasureCodeConfigurer configurer = new ErasureCodeConfigurer();
    Map<String,String> options = Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10M");
    configurer.init(createInitParams(options));

    InputParameters params = createInputParams(List.of(20_000_000L), "inherit");
    var overrides = configurer.override(params);

    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
  }

  @Test
  public void testAtThresholdExactly() {
    ErasureCodeConfigurer configurer = new ErasureCodeConfigurer();
    Map<String,String> options = Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10M");
    configurer.init(createInitParams(options));

    InputParameters params = createInputParams(List.of(10_485_760L), "disable");
    var overrides = configurer.override(params);

    assertEquals("enable",
        overrides.getOverrides().get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
  }

  @Test
  public void testBelowThresholdWithEnable() {
    ErasureCodeConfigurer configurer = new ErasureCodeConfigurer();
    Map<String,String> options = Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10M");
    configurer.init(createInitParams(options));

    InputParameters params = createInputParams(List.of(5_000_000L), "enable");
    var overrides = configurer.override(params);

    assertEquals("disable",
        overrides.getOverrides().get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
  }

  @Test
  public void testBelowThresholdWithDisable() {
    ErasureCodeConfigurer configurer = new ErasureCodeConfigurer();
    Map<String,String> options = Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10M");
    configurer.init(createInitParams(options));

    InputParameters params = createInputParams(List.of(5_000_000L), "disable");
    var overrides = configurer.override(params);

    assertEquals("disable",
        overrides.getOverrides().get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
  }

  @Test
  public void testBelowThresholdWithInherit() {
    ErasureCodeConfigurer configurer = new ErasureCodeConfigurer();
    Map<String,String> options = Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10M");
    configurer.init(createInitParams(options));

    InputParameters params = createInputParams(List.of(5_000_000L), "inherit");
    var overrides = configurer.override(params);

    assertEquals("disable",
        overrides.getOverrides().get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
  }

  @Test
  public void testPolicySet() {
    ErasureCodeConfigurer configurer = new ErasureCodeConfigurer();
    Map<String,String> options = new HashMap<>();
    options.put(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10M");
    options.put(ErasureCodeConfigurer.ERASURE_CODE_POLICY, "RS-3-2-1024k");
    configurer.init(createInitParams(options));

    InputParameters params = createInputParams(List.of(20_000_000L), "enable");
    var overrides = configurer.override(params);

    assertEquals("enable",
        overrides.getOverrides().get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    assertEquals("RS-3-2-1024k",
        overrides.getOverrides().get(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
  }

  @Test
  public void testPolicyNotSetWhenDisabling() {
    ErasureCodeConfigurer configurer = new ErasureCodeConfigurer();
    Map<String,String> options = new HashMap<>();
    options.put(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10M");
    options.put(ErasureCodeConfigurer.ERASURE_CODE_POLICY, "RS-3-2-1024k");
    configurer.init(createInitParams(options));

    InputParameters params = createInputParams(List.of(5_000_000L), "disable");
    var overrides = configurer.override(params);

    assertEquals("disable",
        overrides.getOverrides().get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
  }

  @Test
  public void testPolicyNotSetWithInherit() {
    ErasureCodeConfigurer configurer = new ErasureCodeConfigurer();
    Map<String,String> options = new HashMap<>();
    options.put(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10M");
    options.put(ErasureCodeConfigurer.ERASURE_CODE_POLICY, "RS-3-2-1024k");
    configurer.init(createInitParams(options));

    InputParameters params = createInputParams(List.of(20_000_000L), "inherit");
    var overrides = configurer.override(params);

    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    assertFalse(overrides.getOverrides().containsKey(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
  }
}
