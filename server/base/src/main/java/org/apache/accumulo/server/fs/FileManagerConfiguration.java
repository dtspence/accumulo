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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration.Deriver;
import org.apache.accumulo.core.conf.Property;

class FileManagerConfiguration {
  private final int maxOpen;
  private final long maxIdleMillis;
  private final long slowFilePermitMillis;

  FileManagerConfiguration(int maxOpen, long maxIdleMillis, long slowFilePermitMillis) {
    this.maxOpen = maxOpen;
    this.maxIdleMillis = maxIdleMillis;
    this.slowFilePermitMillis = slowFilePermitMillis;
  }

  int getMaxOpen() {
    return maxOpen;
  }

  long getMaxIdleMillis() {
    return maxIdleMillis;
  }

  long getSlowFilePermitMillis() {
    return slowFilePermitMillis;
  }

  static Deriver<FileManagerConfiguration> newDeriver(AccumuloConfiguration conf) {
    return conf
        .newDeriver(c -> new FileManagerConfiguration(c.getCount(Property.TSERV_SCAN_MAX_OPENFILES),
            c.getTimeInMillis(Property.TSERV_MAX_IDLE),
            c.getTimeInMillis(Property.TSERV_SLOW_FILEPERMIT_MILLIS)));
  }
}
