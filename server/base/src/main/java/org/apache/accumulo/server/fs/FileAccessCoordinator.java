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

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.StampedLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

class FileAccessCoordinator {

  private final static Logger log = LoggerFactory.getLogger(FileAccessCoordinator.class);

  private final static float CACHE_THRESHOLD_MULTIPLIER = 0.1f;

  private final PermitSemaphore semaphore;
  private final StampedLock updateLk;
  private volatile Cache<String,Long> fileLengthCache;
  private volatile long fileLengthCacheMaximumSize;
  private volatile int maxOpen;

  static class ResourceState {
    private final Cache<String,Long> fileLengthCache;
    private final int maxOpen;

    ResourceState(Cache<String,Long> fileLengthCache, int maxOpen) {
      if (maxOpen <= 0) {
        throw new IllegalArgumentException("maxOpen must be > 0");
      }
      this.fileLengthCache = fileLengthCache;
      this.maxOpen = maxOpen;
    }

    public Cache<String,Long> getFileLengthCache() {
      return fileLengthCache;
    }

    public int getMaxOpen() {
      return maxOpen;
    }
  }

  FileAccessCoordinator(int maxOpen) {
    if (maxOpen <= 0) {
      throw new IllegalArgumentException("maxOpen must be > 0");
    }
    this.updateLk = new StampedLock();
    this.maxOpen = maxOpen;
    this.fileLengthCacheMaximumSize = computeCacheMaximumSize(maxOpen);
    this.fileLengthCache = newFileLenCache(fileLengthCacheMaximumSize);
    this.semaphore = new PermitSemaphore(maxOpen, true);

  }

  ResourceState acquireUninterruptibly(int value) {
    semaphore.acquireUninterruptibly(value);

    long stamp = updateLk.tryOptimisticRead();
    int maxOpenLocal = maxOpen;
    Cache<String,Long> cacheLocal = fileLengthCache;

    if (!updateLk.validate(stamp)) {
      try {
        stamp = updateLk.readLock();
        maxOpenLocal = maxOpen;
        cacheLocal = fileLengthCache;
      } finally {
        updateLk.unlockRead(stamp);
      }
    }

    return new ResourceState(cacheLocal, maxOpenLocal);
  }

  void release(int value) {
    semaphore.release(value);
  }

  ResourceState getState() {
    long stamp = updateLk.tryOptimisticRead();
    int maxOpenLocal = maxOpen;
    Cache<String,Long> cacheLocal = fileLengthCache;

    if (!updateLk.validate(stamp)) {
      try {
        stamp = updateLk.readLock();
        maxOpenLocal = maxOpen;
        cacheLocal = fileLengthCache;
      } finally {
        updateLk.unlockRead(stamp);
      }
    }

    return new ResourceState(cacheLocal, maxOpenLocal);
  }

  Cache<String,Long> getFileLengthCache() {
    return fileLengthCache;
  }

  int getMaxOpen() {
    return maxOpen;
  }

  int getOpenPermits() {
    long stamp = updateLk.tryOptimisticRead();
    int maxOpenLocal = maxOpen;
    int availablePermits = semaphore.availablePermits();
    if (!updateLk.validate(stamp)) {
      try {
        stamp = updateLk.readLock();
        maxOpenLocal = maxOpen;
        availablePermits = semaphore.availablePermits();

      } finally {
        updateLk.unlockRead(stamp);
      }
    }
    return maxOpenLocal - availablePermits;
  }

  void resetConfiguration(int maxOpenNew) {
    long stamp = updateLk.writeLock();
    try {
      log.trace("File resource configuration changing: maxOpen {} -> {}", maxOpen, maxOpenNew);
      maxOpen = maxOpenNew;
      semaphore.resizePermits(maxOpenNew);
      long newCacheMaximumSize = computeCacheMaximumSize(maxOpen);
      // Evaluate/estimate cache size change, if size is not changing
      // by a certain threshold, then return current cache
      if (Math.abs(newCacheMaximumSize - fileLengthCacheMaximumSize)
          > fileLengthCacheMaximumSize * CACHE_THRESHOLD_MULTIPLIER) {
        Cache<String,Long> newCache = newFileLenCache(newCacheMaximumSize);
        newCache.putAll(fileLengthCache.asMap());
        fileLengthCache = newCache;
        fileLengthCacheMaximumSize = newCacheMaximumSize;
      }
    } finally {
      updateLk.unlockWrite(stamp);
    }
  }

  private static long computeCacheMaximumSize(long maxOpen) {
    // Previous formula for sizing file length cache
    return Math.min(maxOpen * 1000L, 100_000);
  }

  private static Cache<String,Long> newFileLenCache(long maximumSize) {
    return CacheBuilder.newBuilder().maximumSize(maximumSize).build();
  }

  static class PermitSemaphore extends Semaphore {
    private static final long serialVersionUID = 5950940884872048773L;

    private int maxOpen;

    PermitSemaphore(int maxOpen, boolean fair) {
      super(maxOpen, fair);
      this.maxOpen = maxOpen;
    }

    synchronized int getOpenPermits() {
      return maxOpen - availablePermits();
    }

    synchronized void resizePermits(int resetTo) {
      if (resetTo == maxOpen) {
        return;
      } else if (resetTo <= 0) {
        throw new IllegalArgumentException("resetTo must be positive: " + resetTo);
      }
      log.trace("File permits resizing from {} to {}", maxOpen, resetTo);
      int delta = Math.abs(maxOpen - resetTo);
      if (resetTo > maxOpen) {
        release(delta);
        maxOpen = resetTo;
      } else if (resetTo < maxOpen) {
        reducePermits(delta);
        maxOpen = resetTo;
      }
      log.trace("File permits resized to {}", maxOpen);
    }
  }
}
