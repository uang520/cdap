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

package co.cask.cdap.datapipeline;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Test serialization and deserialization of {@link TriggeringPipelinePropertyId} objects with
 * {@link TriggeringPipelinePropertyIdCodec}
 */
public class TriggeringPipelinePropertyCodecTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(TriggeringPipelinePropertyId.class, new TriggeringPipelinePropertyIdCodec())
    .create();
  private static final Type PROPERTY_ID_STRING_MAP =
    new TypeToken<Map<TriggeringPipelinePropertyId, String>>() { }.getType();
  private static final String RUNTIME_ARG_V1 = "raV1";
  private static final String RUNTIME_ARG_V2 = "raV2";
  private static final String PLUGIN_PROPERTY_V1 = "ppV1";
  private static final String PLUGIN_PROPERTY_V2 = "ppV2";
  private static final String TOKEN_V1 = "tkV1";
  private static final String TOKEN_V2 = "tkV2";


  @Test
  public void testSerDeserPropertyIdMap() {
    TriggeringPipelineRuntimeArgId runtimeArgId1 = new TriggeringPipelineRuntimeArgId("ns1", "p1", "runtimeArgsKey");
    TriggeringPipelineRuntimeArgId runtimeArgId2 = new TriggeringPipelineRuntimeArgId("ns1", "p2", "runtimeArgsKey");
    TriggeringPipelinePluginPropertyId pluginPropertyId1 =
      new TriggeringPipelinePluginPropertyId("ns1", "p1", "name1", "key1");
    TriggeringPipelinePluginPropertyId pluginPropertyId2 =
      new TriggeringPipelinePluginPropertyId("ns2", "p1", "name1", "key1");
    TriggeringPipelineTokenId tokenId1 = new TriggeringPipelineTokenId("ns1", "p1", "name1", "key1");
    TriggeringPipelineTokenId tokenId2 = new TriggeringPipelineTokenId("ns2", "p1", "name1", "key1");

    Map<TriggeringPipelinePropertyId, String> expectedPropertiesMap =
      ImmutableMap.<TriggeringPipelinePropertyId, String>builder()
      .put(runtimeArgId1, RUNTIME_ARG_V1)
      .put(runtimeArgId2, RUNTIME_ARG_V2)
      .put(pluginPropertyId1, PLUGIN_PROPERTY_V1)
      .put(pluginPropertyId2, PLUGIN_PROPERTY_V2)
      .put(tokenId1, TOKEN_V1)
      .put(tokenId2, TOKEN_V2)
      .build();
    String propertiesMappingJson = GSON.toJson(expectedPropertiesMap);
    Map<TriggeringPipelinePropertyId, String> propertiesMap = GSON.fromJson(propertiesMappingJson,
                                                                            PROPERTY_ID_STRING_MAP);
    Assert.assertEquals(expectedPropertiesMap, propertiesMap);
  }
}
