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
package org.apache.accumulo.server.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.google.common.cache.Cache;

public class FileAccessCoordinatorTest {

  @Test
  public void testInitialization() {
    FileAccessCoordinator manager = new FileAccessCoordinator(100);

    assertNotNull(manager);
    assertEquals(100, manager.getMaxOpen());
    assertNotNull(manager.getFileLengthCache());
    assertEquals(0, manager.getOpenPermits());
  }

  @Test
  public void testConstructorValidation() {
    assertThrows(IllegalArgumentException.class, () -> {
      new FileAccessCoordinator(0);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new FileAccessCoordinator(-1);
    });
  }

  @Test
  public void testAcquireAndRelease() {
    FileAccessCoordinator manager = new FileAccessCoordinator(10);

    FileAccessCoordinator.ResourceState state = manager.acquireUninterruptibly(3);
    assertNotNull(state);
    assertEquals(10, state.getMaxOpen());
    assertNotNull(state.getFileLengthCache());
    assertEquals(3, manager.getOpenPermits());

    manager.release(2);
    assertEquals(1, manager.getOpenPermits());

    manager.release(1);
    assertEquals(0, manager.getOpenPermits());
  }

  @Test
  public void testGetState() {
    FileAccessCoordinator manager = new FileAccessCoordinator(50);

    FileAccessCoordinator.ResourceState state = manager.getState();
    assertNotNull(state);
    assertEquals(50, state.getMaxOpen());
    assertNotNull(state.getFileLengthCache());
  }

  @Test
  public void testResourceStateValidation() {
    assertThrows(IllegalArgumentException.class, () -> {
      new FileAccessCoordinator.ResourceState(null, 0);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new FileAccessCoordinator.ResourceState(null, -1);
    });
  }

  @Test
  public void testResetConfigurationIncreasePermits() {
    FileAccessCoordinator manager = new FileAccessCoordinator(10);

    manager.acquireUninterruptibly(5);
    assertEquals(5, manager.getOpenPermits());

    FileManagerConfiguration newConfig = new FileManagerConfiguration(20, 5000L, 30000L);
    manager.resetConfiguration(newConfig.getMaxOpen());
    assertEquals(20, manager.getMaxOpen());
    assertEquals(5, manager.getOpenPermits());

    manager.acquireUninterruptibly(10);
    assertEquals(15, manager.getOpenPermits());

    manager.release(15);
    assertEquals(0, manager.getOpenPermits());
  }

  @Test
  public void testResetConfigurationDecreasePermits() {
    FileAccessCoordinator manager = new FileAccessCoordinator(20);

    manager.acquireUninterruptibly(5);
    assertEquals(5, manager.getOpenPermits());

    FileManagerConfiguration newConfig = new FileManagerConfiguration(10, 5000L, 30000L);
    manager.resetConfiguration(newConfig.getMaxOpen());
    assertEquals(10, manager.getMaxOpen());
    assertEquals(5, manager.getOpenPermits());

    manager.acquireUninterruptibly(5);
    assertEquals(10, manager.getOpenPermits());

    manager.release(10);
    assertEquals(0, manager.getOpenPermits());
  }

  @Test
  public void testCacheResizingWithSignificantChange() {
    FileAccessCoordinator manager = new FileAccessCoordinator(10);

    Cache<String,Long> initialCache = manager.getFileLengthCache();
    initialCache.put("testFile", 1234L);

    FileManagerConfiguration newConfig = new FileManagerConfiguration(1000, 5000L, 30000L);
    manager.resetConfiguration(newConfig.getMaxOpen());

    Cache<String,Long> newCache = manager.getFileLengthCache();
    assertNotSame(initialCache, newCache);
    assertEquals(1234L, newCache.getIfPresent("testFile"));
  }

  @Test
  public void testCacheNotResizedForSmallChange() {
    FileAccessCoordinator manager = new FileAccessCoordinator(100);

    Cache<String,Long> initialCache = manager.getFileLengthCache();

    FileManagerConfiguration newConfig = new FileManagerConfiguration(105, 5000L, 30000L);
    manager.resetConfiguration(newConfig.getMaxOpen());

    Cache<String,Long> newCache = manager.getFileLengthCache();
    assertSame(initialCache, newCache);
  }

  @Test
  public void testConcurrentAcquisitionAndConfigurationChange() throws InterruptedException {
    FileAccessCoordinator manager = new FileAccessCoordinator(10);

    ExecutorService executor = Executors.newFixedThreadPool(5);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completeLatch = new CountDownLatch(5);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicBoolean configChanged = new AtomicBoolean(false);

    for (int i = 0; i < 5; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          FileAccessCoordinator.ResourceState state = manager.acquireUninterruptibly(2);
          successCount.incrementAndGet();

          if (configChanged.get()) {
            assertTrue(state.getMaxOpen() == 10 || state.getMaxOpen() == 20);
          }

          Thread.sleep(10);
          manager.release(2);
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          completeLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    Thread.sleep(50);

    FileManagerConfiguration newConfig = new FileManagerConfiguration(20, 5000L, 30000L);
    manager.resetConfiguration(newConfig.getMaxOpen());
    configChanged.set(true);

    assertTrue(completeLatch.await(5, TimeUnit.SECONDS));
    assertEquals(5, successCount.get());
    assertEquals(0, manager.getOpenPermits());

    executor.shutdown();
  }

  @Test
  public void testResourceStateConsistencyDuringReads() throws InterruptedException {
    FileAccessCoordinator manager = new FileAccessCoordinator(100);
    ExecutorService executor = Executors.newFixedThreadPool(10);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completeLatch = new CountDownLatch(20);
    AtomicReference<Exception> error = new AtomicReference<>();

    for (int i = 0; i < 10; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < 100; j++) {
            FileAccessCoordinator.ResourceState state = manager.getState();
            assertNotNull(state.getFileLengthCache());
            assertTrue(state.getMaxOpen() > 0);
          }
        } catch (Exception e) {
          error.set(e);
        } finally {
          completeLatch.countDown();
        }
      });
    }

    for (int i = 0; i < 10; i++) {
      final int newSize = 10 + (i + 1) * 5;
      executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(10);
          FileManagerConfiguration newConfig = new FileManagerConfiguration(newSize, 5000L, 30000L);
          manager.resetConfiguration(newConfig.getMaxOpen());
        } catch (Exception e) {
          error.set(e);
        } finally {
          completeLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(completeLatch.await(5, TimeUnit.SECONDS));

    if (error.get() != null) {
      throw new AssertionError("Test failed with error", error.get());
    }

    executor.shutdown();
  }

  @Test
  public void testMultipleAcquireReleaseCycles() {
    FileAccessCoordinator manager = new FileAccessCoordinator(5);

    for (int i = 0; i < 3; i++) {
      FileAccessCoordinator.ResourceState state = manager.acquireUninterruptibly(2);
      assertEquals(2, manager.getOpenPermits());
      assertNotNull(state.getFileLengthCache());
      assertEquals(5, state.getMaxOpen());

      manager.release(2);
      assertEquals(0, manager.getOpenPermits());
    }
  }

  @Test
  public void testAcquireAllPermits() {
    FileAccessCoordinator manager = new FileAccessCoordinator(10);

    FileAccessCoordinator.ResourceState state = manager.acquireUninterruptibly(10);
    assertEquals(10, manager.getOpenPermits());
    assertEquals(10, state.getMaxOpen());

    manager.release(10);
    assertEquals(0, manager.getOpenPermits());
  }

  @Test
  public void testConcurrentGetState() throws InterruptedException {
    FileAccessCoordinator manager = new FileAccessCoordinator(50);
    ExecutorService executor = Executors.newFixedThreadPool(20);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completeLatch = new CountDownLatch(20);
    AtomicInteger stateReadCount = new AtomicInteger(0);

    for (int i = 0; i < 20; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < 100; j++) {
            FileAccessCoordinator.ResourceState state = manager.getState();
            assertNotNull(state);
            assertNotNull(state.getFileLengthCache());
            assertEquals(50, state.getMaxOpen());
            stateReadCount.incrementAndGet();
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          completeLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(completeLatch.await(5, TimeUnit.SECONDS));
    assertEquals(2000, stateReadCount.get());

    executor.shutdown();
  }

  @Test
  public void testAcquireBlocksWhenInsufficientPermits() throws InterruptedException {
    FileAccessCoordinator manager = new FileAccessCoordinator(5);
    manager.acquireUninterruptibly(5);

    AtomicBoolean acquired = new AtomicBoolean(false);
    Thread t = new Thread(() -> {
      manager.acquireUninterruptibly(1);
      acquired.set(true);
      manager.release(1);
    });

    t.start();
    Thread.sleep(100);
    assertFalse(acquired.get());

    manager.release(1);
    t.join(1000);
    assertTrue(acquired.get());
  }

  @Test
  public void testResourceStateConsistencyDuringReset() {
    FileAccessCoordinator manager = new FileAccessCoordinator(10);
    FileAccessCoordinator.ResourceState state1 = manager.acquireUninterruptibly(3);

    manager.resetConfiguration(20);
    FileAccessCoordinator.ResourceState state2 = manager.acquireUninterruptibly(5);

    assertEquals(10, state1.getMaxOpen());
    assertNotNull(state1.getFileLengthCache());

    assertEquals(20, state2.getMaxOpen());
    assertNotNull(state2.getFileLengthCache());

    manager.release(8);
    assertEquals(0, manager.getOpenPermits());
  }

  @Test
  public void testAcquireZeroPermits() {
    FileAccessCoordinator manager = new FileAccessCoordinator(10);
    FileAccessCoordinator.ResourceState state = manager.acquireUninterruptibly(0);

    assertNotNull(state);
    assertEquals(0, manager.getOpenPermits());

    manager.release(0);
    assertEquals(0, manager.getOpenPermits());
  }

  @Test
  public void testGetOpenPermitsAccuracyDuringConcurrentOperations() throws InterruptedException {
    FileAccessCoordinator manager = new FileAccessCoordinator(100);
    ExecutorService executor = Executors.newFixedThreadPool(10);
    CountDownLatch latch = new CountDownLatch(10);

    for (int i = 0; i < 10; i++) {
      executor.submit(() -> {
        try {
          for (int j = 0; j < 100; j++) {
            manager.acquireUninterruptibly(1);
            int open = manager.getOpenPermits();
            assertTrue(open >= 1 && open <= 100);
            manager.release(1);
          }
        } finally {
          latch.countDown();
        }
      });
    }

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(0, manager.getOpenPermits());
    executor.shutdown();
  }

  @Test
  public void testCacheEntriesPreservedWhenNotResized() {
    FileAccessCoordinator manager = new FileAccessCoordinator(100);
    Cache<String,Long> cache = manager.getFileLengthCache();

    cache.put("file1", 1000L);
    cache.put("file2", 2000L);

    manager.resetConfiguration(105);

    Cache<String,Long> sameCache = manager.getFileLengthCache();
    assertSame(cache, sameCache);
    assertEquals(1000L, sameCache.getIfPresent("file1"));
    assertEquals(2000L, sameCache.getIfPresent("file2"));
  }

  @Test
  public void testConfigIncreaseUnblocksWaitingThreads() throws InterruptedException {
    FileAccessCoordinator manager = new FileAccessCoordinator(5);
    manager.acquireUninterruptibly(5);

    CountDownLatch waitingLatch = new CountDownLatch(3);
    CountDownLatch acquiredLatch = new CountDownLatch(3);

    for (int i = 0; i < 3; i++) {
      new Thread(() -> {
        waitingLatch.countDown();
        manager.acquireUninterruptibly(2);
        acquiredLatch.countDown();
        manager.release(2);
      }).start();
    }

    assertTrue(waitingLatch.await(1, TimeUnit.SECONDS));
    Thread.sleep(100);

    manager.resetConfiguration(15);
    manager.release(5);

    assertTrue(acquiredLatch.await(2, TimeUnit.SECONDS));
  }

  @Test
  public void testRapidSuccessiveResets() {
    FileAccessCoordinator manager = new FileAccessCoordinator(10);

    for (int i = 0; i < 100; i++) {
      manager.resetConfiguration(10 + (i % 50));
      assertTrue(manager.getMaxOpen() >= 10 && manager.getMaxOpen() < 60);
    }
  }
}
