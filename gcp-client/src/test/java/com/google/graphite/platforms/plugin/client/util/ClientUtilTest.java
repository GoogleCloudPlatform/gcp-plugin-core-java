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

package com.google.graphite.platforms.plugin.client.util;

import static com.google.graphite.platforms.plugin.client.util.ClientUtil.processResourceList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.compute.model.Zone;
import com.google.common.collect.ImmutableList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Test suite for {@link ClientUtil}. */
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

  @Test
  public void testLabelsToFilterString() {
    Map<String, String> labels = new LinkedHashMap<>();
    labels.put("key1", "value1");
    labels.put("key2", "value2");
    String expect = "(labels.key1 eq value1) (labels.key2 eq value2)";

    String got = ClientUtil.buildLabelsFilterString(labels);
    assertEquals(expect, got);
  }
}
