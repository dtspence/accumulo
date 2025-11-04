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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

public class PermitSemaphoreTest {

  @Test
  public void testSemaphoreIncreaseWhileAcquired() {
    FileAccessCoordinator.PermitSemaphore semaphore =
        new FileAccessCoordinator.PermitSemaphore(10, true);

    semaphore.acquireUninterruptibly(7);
    assertEquals(7, semaphore.getOpenPermits());
    assertEquals(3, semaphore.availablePermits());

    semaphore.resizePermits(15);

    assertEquals(7, semaphore.getOpenPermits());
    assertEquals(8, semaphore.availablePermits());

    semaphore.acquireUninterruptibly(8);
    assertEquals(15, semaphore.getOpenPermits());
    assertEquals(0, semaphore.availablePermits());

    semaphore.release(15);
    assertEquals(0, semaphore.getOpenPermits());
    assertEquals(15, semaphore.availablePermits());
  }

  @Test
  public void testSemaphoreDecreaseWhileAcquired() {
    FileAccessCoordinator.PermitSemaphore semaphore =
        new FileAccessCoordinator.PermitSemaphore(20, true);

    semaphore.acquireUninterruptibly(10);
    assertEquals(10, semaphore.getOpenPermits());
    assertEquals(10, semaphore.availablePermits());

    semaphore.resizePermits(15);

    assertEquals(10, semaphore.getOpenPermits());
    assertEquals(5, semaphore.availablePermits());

    semaphore.acquireUninterruptibly(5);
    assertEquals(15, semaphore.getOpenPermits());
    assertEquals(0, semaphore.availablePermits());

    semaphore.release(10);
    assertEquals(5, semaphore.getOpenPermits());
    assertEquals(10, semaphore.availablePermits());
  }

  @Test
  public void testSemaphoreDecreaseBlocksNewAcquisitions() throws InterruptedException {
    FileAccessCoordinator.PermitSemaphore semaphore =
        new FileAccessCoordinator.PermitSemaphore(20, true);

    semaphore.acquireUninterruptibly(10);

    semaphore.resizePermits(10);

    assertEquals(10, semaphore.getOpenPermits());
    assertEquals(0, semaphore.availablePermits());

    AtomicBoolean acquired = new AtomicBoolean(false);
    CountDownLatch startedLatch = new CountDownLatch(1);
    Thread acquirer = new Thread(() -> {
      startedLatch.countDown();
      semaphore.acquireUninterruptibly(1);
      acquired.set(true);
    });

    acquirer.start();
    assertTrue(startedLatch.await(1, TimeUnit.SECONDS));
    Thread.sleep(100);
    assertEquals(false, acquired.get());

    semaphore.release(1);
    acquirer.join(1000);
    assertTrue(acquired.get());

    assertEquals(10, semaphore.getOpenPermits());
    assertEquals(0, semaphore.availablePermits());
  }

  @Test
  public void testResizeToSameValue() {
    FileAccessCoordinator.PermitSemaphore semaphore =
        new FileAccessCoordinator.PermitSemaphore(10, true);

    semaphore.acquireUninterruptibly(5);
    assertEquals(5, semaphore.getOpenPermits());

    semaphore.resizePermits(10);

    assertEquals(5, semaphore.getOpenPermits());
    assertEquals(5, semaphore.availablePermits());
  }

  @Test
  public void testResizeWithAllPermitsAcquired() {
    FileAccessCoordinator.PermitSemaphore semaphore =
        new FileAccessCoordinator.PermitSemaphore(10, true);

    semaphore.acquireUninterruptibly(10);
    assertEquals(10, semaphore.getOpenPermits());
    assertEquals(0, semaphore.availablePermits());

    semaphore.resizePermits(20);

    assertEquals(10, semaphore.getOpenPermits());
    assertEquals(10, semaphore.availablePermits());

    semaphore.resizePermits(5);

    assertEquals(10, semaphore.getOpenPermits());
    assertEquals(-5, semaphore.availablePermits());

    semaphore.release(10);
    assertEquals(0, semaphore.getOpenPermits());
    assertEquals(5, semaphore.availablePermits());
  }

  @Test
  public void testInvalidResizeValues() {
    FileAccessCoordinator.PermitSemaphore semaphore =
        new FileAccessCoordinator.PermitSemaphore(10, true);

    IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class, () -> {
      semaphore.resizePermits(0);
    });
    assertTrue(ex1.getMessage().contains("resetTo must be positive"));

    IllegalArgumentException ex2 = assertThrows(IllegalArgumentException.class, () -> {
      semaphore.resizePermits(-10);
    });
    assertTrue(ex2.getMessage().contains("resetTo must be positive"));
  }

  @Test
  public void testConcurrentAcquisitionAndResize() throws InterruptedException {
    FileAccessCoordinator.PermitSemaphore semaphore =
        new FileAccessCoordinator.PermitSemaphore(10, true);

    ExecutorService executor = Executors.newFixedThreadPool(20);
    CyclicBarrier barrier = new CyclicBarrier(20);
    CountDownLatch completeLatch = new CountDownLatch(20);
    AtomicInteger errors = new AtomicInteger(0);

    for (int i = 0; i < 10; i++) {
      executor.submit(() -> {
        try {
          barrier.await(1, TimeUnit.SECONDS);
          for (int j = 0; j < 10; j++) {
            semaphore.acquireUninterruptibly(1);
            Thread.sleep(5);
            semaphore.release(1);
          }
        } catch (Exception e) {
          errors.incrementAndGet();
        } finally {
          completeLatch.countDown();
        }
      });
    }

    for (int i = 0; i < 10; i++) {
      final int newSize = 5 + (i + 1) * 2;
      executor.submit(() -> {
        try {
          barrier.await(1, TimeUnit.SECONDS);
          Thread.sleep(10);
          semaphore.resizePermits(newSize);
        } catch (Exception e) {
          errors.incrementAndGet();
        } finally {
          completeLatch.countDown();
        }
      });
    }

    assertTrue(completeLatch.await(5, TimeUnit.SECONDS));
    assertEquals(0, errors.get());

    assertTrue(semaphore.getOpenPermits() >= 0);
    assertTrue(semaphore.availablePermits() >= 0);

    executor.shutdown();
  }

  @Test
  public void testResizeUnderHighContention() throws InterruptedException {
    FileAccessCoordinator.PermitSemaphore semaphore =
        new FileAccessCoordinator.PermitSemaphore(5, true);

    ExecutorService executor = Executors.newFixedThreadPool(50);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completeLatch = new CountDownLatch(50);
    AtomicBoolean stop = new AtomicBoolean(false);
    AtomicInteger successfulAcquisitions = new AtomicInteger(0);

    for (int i = 0; i < 40; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          while (!stop.get()) {
            if (semaphore.tryAcquire(1, 10, TimeUnit.MILLISECONDS)) {
              successfulAcquisitions.incrementAndGet();
              Thread.sleep(1);
              semaphore.release(1);
            }
          }
        } catch (Exception e) {} finally {
          completeLatch.countDown();
        }
      });
    }

    for (int i = 0; i < 10; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < 5; j++) {
            Thread.sleep(20);
            int newSize = 3 + (j * 3);
            semaphore.resizePermits(newSize);
          }
        } catch (Exception e) {} finally {
          completeLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    Thread.sleep(500);
    stop.set(true);

    assertTrue(completeLatch.await(5, TimeUnit.SECONDS));
    assertTrue(successfulAcquisitions.get() > 0);

    int finalOpenPermits = semaphore.getOpenPermits();
    assertTrue(finalOpenPermits >= 0);

    executor.shutdown();
  }

  @Test
  public void testExtremeResize() {
    FileAccessCoordinator.PermitSemaphore semaphore =
        new FileAccessCoordinator.PermitSemaphore(1, true);

    semaphore.resizePermits(1000000);
    assertEquals(0, semaphore.getOpenPermits());
    assertEquals(1000000, semaphore.availablePermits());

    semaphore.acquireUninterruptibly(500000);
    assertEquals(500000, semaphore.getOpenPermits());

    semaphore.resizePermits(1);
    assertEquals(500000, semaphore.getOpenPermits());
    assertEquals(-499999, semaphore.availablePermits());

    semaphore.release(500000);
    assertEquals(0, semaphore.getOpenPermits());
    assertEquals(1, semaphore.availablePermits());
  }

  @Test
  public void testFairnessAfterResize() throws InterruptedException {
    FileAccessCoordinator.PermitSemaphore semaphore =
        new FileAccessCoordinator.PermitSemaphore(1, true);

    semaphore.acquireUninterruptibly(1);

    AtomicInteger order = new AtomicInteger(0);
    CountDownLatch thread1Started = new CountDownLatch(1);
    CountDownLatch thread2Started = new CountDownLatch(1);
    AtomicInteger thread1Order = new AtomicInteger(-1);
    AtomicInteger thread2Order = new AtomicInteger(-1);

    Thread thread1 = new Thread(() -> {
      thread1Started.countDown();
      semaphore.acquireUninterruptibly(1);
      thread1Order.set(order.incrementAndGet());
      semaphore.release(1);
    });

    Thread thread2 = new Thread(() -> {
      thread2Started.countDown();
      semaphore.acquireUninterruptibly(1);
      thread2Order.set(order.incrementAndGet());
      semaphore.release(1);
    });

    thread1.start();
    assertTrue(thread1Started.await(1, TimeUnit.SECONDS));
    Thread.sleep(50);

    thread2.start();
    assertTrue(thread2Started.await(1, TimeUnit.SECONDS));
    Thread.sleep(50);

    semaphore.resizePermits(3);

    semaphore.release(1);

    thread1.join(1000);
    thread2.join(1000);

    assertEquals(1, thread1Order.get());
    assertEquals(2, thread2Order.get());
  }

  @Test
  public void testFairnessWithMultipleWaitingThreads() throws InterruptedException {
    FileAccessCoordinator.PermitSemaphore semaphore =
        new FileAccessCoordinator.PermitSemaphore(1, true);
    semaphore.acquireUninterruptibly(1);

    int threadCount = 10;
    AtomicInteger order = new AtomicInteger(0);
    int[] threadOrders = new int[threadCount];
    CountDownLatch allStarted = new CountDownLatch(threadCount);
    Thread[] threads = new Thread[threadCount];

    for (int i = 0; i < threadCount; i++) {
      final int idx = i;
      threads[i] = new Thread(() -> {
        allStarted.countDown();
        semaphore.acquireUninterruptibly(1);
        threadOrders[idx] = order.incrementAndGet();
        semaphore.release(1);
      });
      threads[i].start();
    }

    assertTrue(allStarted.await(1, TimeUnit.SECONDS));
    Thread.sleep(100);

    semaphore.release(1);

    for (int i = 0; i < threadCount; i++) {
      threads[i].join(1000);
    }

    for (int i = 0; i < threadCount; i++) {
      assertEquals(i + 1, threadOrders[i]);
    }
  }
}
