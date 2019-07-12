/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.graphite.platforms.plugin.client;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;

import com.google.api.services.compute.model.DeprecationStatus;
import com.google.api.services.compute.model.DiskType;
import com.google.api.services.compute.model.InstanceTemplate;
import com.google.api.services.compute.model.MachineType;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.Region;
import com.google.api.services.compute.model.Zone;
import com.google.graphite.platforms.plugin.client.util.ClientUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ComputeClientTest {

  private static final String TEST_PROJECT_ID = "test-project";
  private static final String TEST_TEMPLATE_NAME = "test-template-name";

  @Mock public ComputeWrapper compute;

  @InjectMocks ComputeClient computeClient;

  private List<Region> listOfRegions;
  private List<Zone> listOfZones;
  private List<MachineType> listOfMachineTypes;
  private List<DiskType> listOfDiskTypes;
  private List<InstanceTemplate> listOfInstanceTemplate;

  @Before
  public void init() throws Exception {
    listOfRegions = new ArrayList<>();
    listOfZones = new ArrayList<>();
    listOfMachineTypes = new ArrayList<>();
    listOfDiskTypes = new ArrayList<>();
    listOfInstanceTemplate = new ArrayList<>();

    // Mock regions
    Mockito.when(compute.listRegions(anyString())).thenReturn(listOfRegions);

    // Mock zones
    Mockito.when(compute.listZones(anyString())).thenReturn(listOfZones);

    // Mock machine types
    Mockito.when(compute.listMachineTypes(anyString(), anyString())).thenReturn(listOfMachineTypes);

    // Mock disk types
    Mockito.when(compute.listDiskTypes(anyString(), anyString())).thenReturn(listOfDiskTypes);

    // Mock instance templates
    Mockito.when(compute.listInstanceTemplates(anyString())).thenReturn(listOfInstanceTemplate);
    Mockito.when(compute.getInstanceTemplate(anyString(), anyString()))
        .thenReturn(new InstanceTemplate().setName(TEST_TEMPLATE_NAME));
  }

  @Test
  public void getRegions() throws IOException {
    listOfRegions.clear();
    listOfRegions.add(new Region().setName("us-west1"));
    listOfRegions.add(new Region().setName("eu-central1"));
    listOfRegions.add(new Region().setName("us-central1"));
    listOfRegions.add(
        new Region()
            .setName("us-east1")
            .setDeprecated(new DeprecationStatus().setState("DEPRECATED")));

    assertEquals(3, computeClient.getRegions(TEST_PROJECT_ID).size());
    assertEquals("eu-central1", computeClient.getRegions(TEST_PROJECT_ID).get(0).getName());
  }

  @Test
  public void getZones() throws IOException {
    listOfZones.clear();
    listOfZones.add(new Zone().setRegion("us-west1").setName("us-west1-b"));
    listOfZones.add(new Zone().setRegion("eu-central1").setName("eu-central1-a"));
    listOfZones.add(new Zone().setRegion("us-west1").setName("us-west1-a"));

    assertEquals(2, computeClient.getZones(TEST_PROJECT_ID, "us-west1").size());
    assertEquals(
        "us-west1-a", computeClient.getZones(TEST_PROJECT_ID, "us-west1").get(0).getName());

    listOfZones.clear();
    assertEquals(0, computeClient.getZones(TEST_PROJECT_ID, "us-west1").size());
  }

  @Test
  public void getMachineTypes() throws IOException {
    listOfMachineTypes.clear();
    listOfMachineTypes.add(new MachineType().setName("b"));
    listOfMachineTypes.add(new MachineType().setName("a"));
    listOfMachineTypes.add(new MachineType().setName("z"));
    listOfMachineTypes.add(
        new MachineType()
            .setName("d")
            .setDeprecated(new DeprecationStatus().setState("DEPRECATED")));

    assertEquals(3, computeClient.getMachineTypes(TEST_PROJECT_ID, "test").size());
    assertEquals("a", computeClient.getMachineTypes(TEST_PROJECT_ID, "test").get(0).getName());
  }

  @Test
  public void getDiskTypes() throws IOException {
    listOfDiskTypes.clear();
    listOfDiskTypes.add(new DiskType().setName("b"));
    listOfDiskTypes.add(new DiskType().setName("a"));
    listOfDiskTypes.add(new DiskType().setName("z"));
    listOfDiskTypes.add(new DiskType().setName("local-d"));
    listOfDiskTypes.add(
        new DiskType().setName("d").setDeprecated(new DeprecationStatus().setState("DEPRECATED")));

    assertEquals(3, computeClient.getBootDiskTypes(TEST_PROJECT_ID, "test").size());
    assertEquals("a", computeClient.getBootDiskTypes(TEST_PROJECT_ID, "test").get(0).getName());
  }

  @Test
  public void nameFromSelfLink() {
    String zone;

    zone = "https://www.googleapis.com/compute/v1/projects/evandbrown17/zones/asia-east1-a";
    assertEquals("asia-east1-a", ClientUtil.nameFromSelfLink(zone));

    zone = "asia-east1-a";
    assertEquals("asia-east1-a", ClientUtil.nameFromSelfLink(zone));
  }

  @Test
  public void labelsToFilterString() {
    Map<String, String> labels = new LinkedHashMap<>();
    labels.put("key1", "value1");
    labels.put("key2", "value2");
    String expect = "(labels.key1 eq value1) (labels.key2 eq value2)";

    String got = ClientUtil.buildLabelsFilterString(labels);
    assertEquals(expect, got);
  }

  @Test
  public void mergeMetadataItemsTest() {
    List<Metadata.Items> newItems = new ArrayList<>();
    newItems.add(new Metadata.Items().setKey("ssh-keys").setValue("new"));

    List<Metadata.Items> existingItems = new ArrayList<>();
    existingItems.add(new Metadata.Items().setKey("ssh-keys").setValue("old"));
    existingItems.add(new Metadata.Items().setKey("no-overwrite").setValue("no-overwrite"));

    List<Metadata.Items> merged = ComputeClient.mergeMetadataItems(newItems, existingItems);

    assertEquals(existingItems.size(), merged.size());
  }

  @Test
  public void getTemplates() throws IOException {
    assertEquals(0, computeClient.getTemplates(TEST_PROJECT_ID).size());

    listOfInstanceTemplate.add(new InstanceTemplate().setName("z"));
    listOfInstanceTemplate.add(new InstanceTemplate().setName("a"));
    listOfInstanceTemplate.add(new InstanceTemplate().setName("c"));

    assertEquals(3, computeClient.getTemplates(TEST_PROJECT_ID).size());
    assertEquals("a", computeClient.getTemplates(TEST_PROJECT_ID).get(0).getName());
  }

  @Test
  public void getTemplate() throws IOException {
    assertEquals(
        new InstanceTemplate().setName(TEST_TEMPLATE_NAME),
        computeClient.getTemplate(TEST_PROJECT_ID, TEST_TEMPLATE_NAME));
  }
}
