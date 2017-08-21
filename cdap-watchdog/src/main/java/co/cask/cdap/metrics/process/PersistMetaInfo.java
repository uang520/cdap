/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.metrics.process;


import javax.annotation.Nullable;

/**
 * meta information about messageId, metrics processed by messaging metrics server
 */
public final class PersistMetaInfo {
  private byte[] messageId;
  private Long oldestMetricsTimestamp;
  private Long latestMetricsTimestamp;
  private Long messagesProcessed;

  PersistMetaInfo(byte[] messageId) {
    this.messageId = messageId;
    this.oldestMetricsTimestamp = null;
    this.latestMetricsTimestamp = null;
    this.messagesProcessed = 0L;
  }

  void updateMetaInfo(byte[] messageId, long timestamp) {
    this.messageId = messageId;
    if (oldestMetricsTimestamp == null || timestamp < oldestMetricsTimestamp) {
      oldestMetricsTimestamp = timestamp;
    }
    if (latestMetricsTimestamp == null || timestamp > latestMetricsTimestamp) {
      latestMetricsTimestamp = timestamp;
    }
    messagesProcessed += 1;
  }

  void resetMetaInfo() {
    this.oldestMetricsTimestamp = null;
    this.latestMetricsTimestamp = null;
    this.messagesProcessed = 0L;
  }

  @Nullable
  byte[] getMessageId() {
    return messageId;
  }

  @Nullable
  public Long getOldestMetricsTimestamp() {
    return oldestMetricsTimestamp;
  }

  @Nullable
  public Long getLatestMetricsTimestamp() {
    return latestMetricsTimestamp;
  }

  public long getMessagesProcessed() {
    return messagesProcessed;
  }
}
