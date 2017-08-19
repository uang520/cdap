/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.api.schedule;

/**
 * The stream size trigger information to be passed to the triggered program.
 */
public class StreamSizeTriggerInfo extends TriggerInfo {
  private final String streamNamespace;
  private final String streamName;
  private final int triggerMB;

  public StreamSizeTriggerInfo(String streamNamespace, String streamName, int triggerMB) {
    super(Trigger.Type.STREAM_SIZE);
    this.streamNamespace = streamNamespace;
    this.streamName = streamName;
    this.triggerMB = triggerMB;
  }

  /**
   * @return The namespace of the stream.
   */
  public String getStreamNamespace() {
    return streamNamespace;
  }

  /**
   * @return The name of the stream.
   */
  public String getStreamName() {
    return streamName;
  }

  /**
   * @return The size of data, in MB, that the stream has to receive to trigger the schedule.
   */
  public int getTriggerMB() {
    return triggerMB;
  }
}
