/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.graphite.platforms.plugin.client.util;

import static com.google.cloud.graphite.platforms.plugin.client.util.ClientUtil.nameFromSelfLink;
import static com.google.cloud.graphite.platforms.plugin.client.util.ClientUtil.processResourceList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.compute.model.Zone;
import com.google.cloud.graphite.platforms.plugin.client.model.InstanceResourceData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

/** Tests {@link ClientUtil}. */
public class ClientUtilTest {
  @Test
  public void testProcessResourceListNullItems() {
    List<String> result = processResourceList(null, String::compareTo);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testProcessResourceListEmptyItems() {
    List<String> result = processResourceList(ImmutableList.of(), String::compareTo);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testProcessResourceListWithComparator() {
    List<String> expected = ImmutableList.of("example", "foo", "test");
    List<String> result =
        processResourceList(ImmutableList.of("test", "example", "foo"), String::compareTo);
    assertNotNull(result);
    assertEquals(expected, result);
  }

  @Test
  public void testProcessResourceListWithFilter() {
    Zone zoneA = new Zone().setName("us-west1-a").setRegion("us-west1");
    Zone zoneB = new Zone().setName("us-central1-b").setRegion("us-central1");
    Zone zoneC = new Zone().setName("us-east2-c").setRegion("us-east2");
    List<Zone> expected = ImmutableList.of(zoneB, zoneA);
    List<Zone> result =
        processResourceList(
            ImmutableList.of(zoneA, zoneB, zoneC),
            zone -> !zone.getRegion().equals("us-east2"),
            Comparator.comparing(Zone::getName));
    assertNotNull(result);
    assertEquals(expected, result);
  }

  @Test
  public void testNameFromSelfLink() {
    String zone = "https://www.googleapis.com/compute/v1/projects/evandbrown17/zones/asia-east1-a";
    assertEquals("asia-east1-a", ClientUtil.nameFromSelfLink(zone));
  }

  @Test
  public void testNameFromSelfLinkOnlyName() {
    String zone = "asia-east1-a";
    assertEquals("asia-east1-a", ClientUtil.nameFromSelfLink(zone));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNameFromSelfLinkNull() {
    nameFromSelfLink(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNameFromSelfLinkEmpty() {
    nameFromSelfLink("");
  }

  @Test
  public void testLabelsToFilterString() {
    Map<String, String> labels = new LinkedHashMap<>();
    labels.put("key1", "value1");
    labels.put("key2", "value2");
    String expect = "(labels.key1 eq value1) (labels.key2 eq value2)";

    String got = ClientUtil.buildLabelsFilterString(labels);
    assertEquals(expect, got);
  }

  @Test
  public void testBuildFilterStringNoFilters() {
    Map<String, String> filters = ImmutableMap.<String, String>builder().build();
    String expect = "";
    String got = ClientUtil.buildFilterString(filters);
    assertEquals(expect, got);
  }

  @Test
  public void testBuildFilterStringOneFilter() {
    Map<String, String> filters =
        ImmutableMap.<String, String>builder().put("key", "value").build();
    String expect = "key=\"value\"";
    String got = ClientUtil.buildFilterString(filters);
    assertEquals(expect, got);
  }

  @Test
  public void testBuildFilterStringMultipleFilters() {
    Map<String, String> filters =
        ImmutableMap.<String, String>builder().put("key1", "value1").put("key2", "value2").build();
    String expect = "key1=\"value1\" AND key2=\"value2\"";
    String got = ClientUtil.buildFilterString(filters);
    assertEquals(expect, got);
  }

  @Test
  public void testParseResourceDataParsesSelfLink() {
    Optional<InstanceResourceData> result =
        ClientUtil.parseInstanceResourceData(
            "https://www.googleapis.com/compute/v1/projects/test-project-1/zones/test-zone-1/instances/test-name");
    assertTrue(result.isPresent());
    assertEquals("test-project-1", result.get().getProjectId());
    assertEquals("test-zone-1", result.get().getZone());
    assertEquals("test-name", result.get().getName());
  }

  @Test
  public void testParseResourceDataReturnsEmptyWithInvalidInput() {
    Optional<InstanceResourceData> result = ClientUtil.parseInstanceResourceData("fizz-buzz");
    assertFalse(result.isPresent());
  }
}
