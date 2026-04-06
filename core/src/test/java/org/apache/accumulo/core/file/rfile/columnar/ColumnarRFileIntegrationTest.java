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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheManagerFactory;
import org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCache;
import org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCacheManager;
import org.apache.accumulo.core.file.blockfile.impl.BasicCacheProvider;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.CachableBuilder;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.spi.cache.BlockCacheManager;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.jupiter.api.Test;

/**
 * Integration tests that write RFiles normally, then read them with and without the columnar scan
 * iterator wrapping, verifying identical output.
 */
public class ColumnarRFileIntegrationTest {

  private static final Collection<ByteSequence> EMPTY_CF_SET = Collections.emptyList();
  private static final Configuration hadoopConf = new Configuration();

  static class SeekableByteArrayInputStream extends ByteArrayInputStream
      implements Seekable, PositionedReadable {
    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long pos) throws IOException {
      if (mark != 0) {
        throw new IllegalStateException();
      }
      reset();
      long skipped = skip(pos);
      if (skipped != pos) {
        throw new IOException();
      }
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) {
      if (position >= count) {
        return -1;
      }
      int available = count - (int) position;
      int readLen = Math.min(available, length);
      System.arraycopy(buf, (int) position, buffer, offset, readLen);
      return readLen;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      int read = read(position, buffer, offset, length);
      if (read != length) {
        throw new IOException("Could not read fully");
      }
    }
  }

  static Key newKey(String row, String cf, String cq, String cv, long ts) {
    return new Key(row.getBytes(UTF_8), cf.getBytes(UTF_8), cq.getBytes(UTF_8), cv.getBytes(UTF_8),
        ts);
  }

  static Value newValue(String val) {
    return new Value(val);
  }

  /** Writes an RFile to a byte array. */
  private byte[] writeRFile(List<Key> keys, List<Value> values, int blockSize) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream dos = new FSDataOutputStream(baos, new FileSystem.Statistics("a"));

    DefaultConfiguration dc = DefaultConfiguration.getInstance();
    CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
        dc.getAllCryptoProperties());

    BCFile.Writer bcWriter = new BCFile.Writer(dos, null, "gz", hadoopConf, cs);
    RFile.Writer writer = new RFile.Writer(bcWriter, blockSize, 1000, null, null);
    writer.startDefaultLocalityGroup();

    for (int i = 0; i < keys.size(); i++) {
      writer.append(keys.get(i), values.get(i));
    }

    writer.close();
    dos.close();
    return baos.toByteArray();
  }

  /** Opens a standard RFile reader from byte array data. */
  private RFile.Reader openReader(byte[] data) throws Exception {
    SeekableByteArrayInputStream bais = new SeekableByteArrayInputStream(data);
    FSDataInputStream in = new FSDataInputStream(bais);

    ConfigurationCopy cc = new ConfigurationCopy(DefaultConfiguration.getInstance());
    cc.set(Property.GENERAL_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
    cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(100000));
    cc.set(Property.TSERV_DATACACHE_SIZE, Long.toString(100000000));
    cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(100000000));

    BlockCacheManager manager = BlockCacheManagerFactory.getInstance(cc);
    manager.start(BlockCacheConfiguration.forTabletServer(cc));
    LruBlockCache indexCache = (LruBlockCache) manager.getBlockCache(CacheType.INDEX);
    LruBlockCache dataCache = (LruBlockCache) manager.getBlockCache(CacheType.DATA);

    CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
        DefaultConfiguration.getInstance().getAllCryptoProperties());

    CachableBuilder cb =
        new CachableBuilder().input(in, "source-1").length(data.length).conf(hadoopConf)
            .cacheProvider(new BasicCacheProvider(indexCache, dataCache)).cryptoService(cs);

    return new RFile.Reader(cb);
  }

  /** Wraps a reader with a ColumnarScanIterator using the given threshold. */
  private ColumnarScanIterator wrapWithColumnar(SortedKeyValueIterator<Key,Value> source,
      int batchThreshold) {
    return new ColumnarScanIterator(source, Collections.emptyList(), batchThreshold);
  }

  /** Reads all entries from an iterator after seeking to the given range. */
  private List<Entry> readAll(SortedKeyValueIterator<Key,Value> iter, Range range)
      throws IOException {
    iter.seek(range, EMPTY_CF_SET, false);
    List<Entry> entries = new ArrayList<>();
    while (iter.hasTop()) {
      entries.add(new Entry(new Key(iter.getTopKey()), new Value(iter.getTopValue())));
      iter.next();
    }
    return entries;
  }

  private static class Entry {
    final Key key;
    final Value value;

    Entry(Key key, Value value) {
      this.key = key;
      this.value = value;
    }
  }

  private void assertEntriesEqual(List<Entry> expected, List<Entry> actual) {
    assertEquals(expected.size(), actual.size(), "Entry count mismatch");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i).key, actual.get(i).key, "Key mismatch at index " + i);
      assertEquals(expected.get(i).value, actual.get(i).value, "Value mismatch at index " + i);
    }
  }

  @Test
  public void testColumnarMatchesNonColumnarFullScan() throws Exception {
    List<Key> keys = new ArrayList<>();
    List<Value> values = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      keys.add(newKey(String.format("row%05d", i), "cf", "cq", "", 1000));
      values.add(newValue("value" + i));
    }

    // Small block size to force multiple blocks
    byte[] data = writeRFile(keys, values, 500);

    // Read without columnar
    RFile.Reader nonColumnar = openReader(data);
    List<Entry> expected = readAll(nonColumnar, new Range());
    nonColumnar.close();

    // Read with columnar wrapper (threshold=1 to always use large batches)
    RFile.Reader baseReader = openReader(data);
    ColumnarScanIterator columnar = wrapWithColumnar(baseReader, 1);
    List<Entry> actual = readAll(columnar, new Range());
    baseReader.close();

    assertEntriesEqual(expected, actual);
    assertEquals(500, actual.size());
  }

  @Test
  public void testColumnarMatchesNonColumnarWithSeeks() throws Exception {
    List<Key> keys = new ArrayList<>();
    List<Value> values = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      keys.add(newKey(String.format("row%05d", i), "cf", "cq", "", 1000));
      values.add(newValue("value" + i));
    }

    byte[] data = writeRFile(keys, values, 500);

    // Test seeking to middle of data
    Range range = new Range(newKey("row00050", "cf", "cq", "", 1000),
        newKey("row00150", "cf", "cq", "", 1000));

    RFile.Reader nonColumnar = openReader(data);
    List<Entry> expected = readAll(nonColumnar, range);
    nonColumnar.close();

    RFile.Reader baseReader = openReader(data);
    ColumnarScanIterator columnar = wrapWithColumnar(baseReader, 1);
    List<Entry> actual = readAll(columnar, range);
    baseReader.close();

    assertEntriesEqual(expected, actual);
    assertTrue(actual.size() > 0);
    assertTrue(actual.size() <= 101);
  }

  @Test
  public void testColumnarEmptyFile() throws Exception {
    byte[] data = writeRFile(Collections.emptyList(), Collections.emptyList(), 1000);

    RFile.Reader baseReader = openReader(data);
    ColumnarScanIterator columnar = wrapWithColumnar(baseReader, 1);
    List<Entry> entries = readAll(columnar, new Range());
    baseReader.close();

    assertTrue(entries.isEmpty());
  }

  @Test
  public void testColumnarSingleEntry() throws Exception {
    List<Key> keys = List.of(newKey("row", "cf", "cq", "", 1000));
    List<Value> values = List.of(newValue("val"));

    byte[] data = writeRFile(keys, values, 1000);

    RFile.Reader nonColumnar = openReader(data);
    List<Entry> expected = readAll(nonColumnar, new Range());
    nonColumnar.close();

    RFile.Reader baseReader = openReader(data);
    ColumnarScanIterator columnar = wrapWithColumnar(baseReader, 1);
    List<Entry> actual = readAll(columnar, new Range());
    baseReader.close();

    assertEntriesEqual(expected, actual);
    assertEquals(1, actual.size());
  }

  @Test
  public void testColumnarHighThresholdBehavesLikeNonColumnar() throws Exception {
    // With a very high threshold, columnar uses small batches but should still produce
    // identical results
    List<Key> keys = new ArrayList<>();
    List<Value> values = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      keys.add(newKey(String.format("row%05d", i), "cf", "cq", "", 1000));
      values.add(newValue("value" + i));
    }

    byte[] data = writeRFile(keys, values, 500);

    RFile.Reader nonColumnar = openReader(data);
    List<Entry> expected = readAll(nonColumnar, new Range());
    nonColumnar.close();

    // High threshold means mostly single-entry batches
    RFile.Reader baseReader = openReader(data);
    ColumnarScanIterator columnar = wrapWithColumnar(baseReader, 99999);
    List<Entry> actual = readAll(columnar, new Range());
    baseReader.close();

    assertEntriesEqual(expected, actual);
  }

  @Test
  public void testColumnarMultipleSeeks() throws Exception {
    // Simulate a pattern of multiple seeks (like a batch scanner)
    List<Key> keys = new ArrayList<>();
    List<Value> values = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      keys.add(newKey(String.format("row%05d", i), "cf", "cq", "", 1000));
      values.add(newValue("value" + i));
    }

    byte[] data = writeRFile(keys, values, 500);

    // Multiple sequential range reads
    Range[] ranges = {
        new Range(newKey("row00000", "", "", "", Long.MAX_VALUE),
            newKey("row00050", "", "", "", 0)),
        new Range(newKey("row00100", "", "", "", Long.MAX_VALUE),
            newKey("row00150", "", "", "", 0)),
        new Range(newKey("row00200", "", "", "", Long.MAX_VALUE),
            newKey("row00250", "", "", "", 0))};

    for (Range range : ranges) {
      RFile.Reader nonColumnar = openReader(data);
      List<Entry> expected = readAll(nonColumnar, range);
      nonColumnar.close();

      RFile.Reader baseReader = openReader(data);
      ColumnarScanIterator columnar = wrapWithColumnar(baseReader, 1);
      List<Entry> actual = readAll(columnar, range);
      baseReader.close();

      assertEntriesEqual(expected, actual);
    }
  }
}
