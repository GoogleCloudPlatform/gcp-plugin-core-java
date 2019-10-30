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

package com.google.cloud.graphite.platforms.plugin.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.services.compute.model.DeprecationStatus;
import com.google.api.services.compute.model.DiskType;
import com.google.api.services.compute.model.InstanceTemplate;
import com.google.api.services.compute.model.MachineType;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.Region;
import com.google.api.services.compute.model.Zone;
import com.google.cloud.graphite.platforms.plugin.client.ComputeClient.GuestAttribute;
import com.google.cloud.graphite.platforms.plugin.client.ComputeClient.GuestAttributeQueryResult;
import com.google.cloud.graphite.platforms.plugin.client.ComputeClient.GuestAttributeQueryValue;
import com.google.cloud.graphite.platforms.plugin.client.ComputeClient.InstanceResourceData;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests {@link com.google.cloud.graphite.platforms.plugin.client.ComputeClient}. */
@RunWith(MockitoJUnitRunner.class)
public class ComputeClientTest {
  private static final String TEST_PROJECT_ID = "test-project";
  private static final String TEST_TEMPLATE_NAME = "test-template-name";
  private static final String TEST_INSTANCE_ID = "test-instance";
  private static final String TEST_ZONE_LINK = "test-zone";

  @Mock private ComputeWrapper compute;
  @InjectMocks private ComputeClient computeClient;
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

    assertEquals(3, computeClient.listRegions(TEST_PROJECT_ID).size());
    assertEquals("eu-central1", computeClient.listRegions(TEST_PROJECT_ID).get(0).getName());
  }

  @Test
  public void getZones() throws IOException {
    listOfZones.clear();
    listOfZones.add(new Zone().setRegion("us-west1").setName("us-west1-b"));
    listOfZones.add(new Zone().setRegion("eu-central1").setName("eu-central1-a"));
    listOfZones.add(new Zone().setRegion("us-west1").setName("us-west1-a"));

    assertEquals(2, computeClient.listZones(TEST_PROJECT_ID, "us-west1").size());
    assertEquals(
        "us-west1-a", computeClient.listZones(TEST_PROJECT_ID, "us-west1").get(0).getName());

    listOfZones.clear();
    assertEquals(0, computeClient.listZones(TEST_PROJECT_ID, "us-west1").size());
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

    assertEquals(3, computeClient.listMachineTypes(TEST_PROJECT_ID, "test").size());
    assertEquals("a", computeClient.listMachineTypes(TEST_PROJECT_ID, "test").get(0).getName());
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

    assertEquals(3, computeClient.listBootDiskTypes(TEST_PROJECT_ID, "test").size());
    assertEquals("a", computeClient.listBootDiskTypes(TEST_PROJECT_ID, "test").get(0).getName());
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
    assertEquals(0, computeClient.listTemplates(TEST_PROJECT_ID).size());

    listOfInstanceTemplate.add(new InstanceTemplate().setName("z"));
    listOfInstanceTemplate.add(new InstanceTemplate().setName("a"));
    listOfInstanceTemplate.add(new InstanceTemplate().setName("c"));

    assertEquals(3, computeClient.listTemplates(TEST_PROJECT_ID).size());
    assertEquals("a", computeClient.listTemplates(TEST_PROJECT_ID).get(0).getName());
  }

  @Test
  public void getTemplate() throws IOException {
    assertEquals(
        new InstanceTemplate().setName(TEST_TEMPLATE_NAME),
        computeClient.getTemplate(TEST_PROJECT_ID, TEST_TEMPLATE_NAME));
  }

  @Test
  public void testGetGuestAttributesSyncReturnsProperly() throws IOException {
    HttpRequestFactory requestFactory = Mockito.mock(HttpRequestFactory.class);
    Mockito.when(compute.getRequestFactory()).thenReturn(requestFactory);
    HttpRequest request = Mockito.mock(HttpRequest.class);
    Mockito.when(requestFactory.buildGetRequest(any())).thenReturn(request);
    HttpResponse response = Mockito.mock(HttpResponse.class);
    Mockito.when(request.execute()).thenReturn(response);
    GuestAttribute guestAttribute =
        GuestAttribute.builder()
            .namespace("test-namespace")
            .key("test-key")
            .value("test-value")
            .build();
    GuestAttributeQueryResult guestAttributeQueryResult =
        GuestAttributeQueryResult.builder()
            .queryValue(
                GuestAttributeQueryValue.builder()
                    .items(new ImmutableList.Builder<GuestAttribute>().add(guestAttribute).build())
                    .build())
            .build();
    Mockito.when(response.parseAs(GuestAttributeQueryResult.class))
        .thenReturn(guestAttributeQueryResult);

    assertEquals(
        ImmutableList.of(guestAttribute),
        computeClient.getGuestAttributesSync(TEST_PROJECT_ID, TEST_ZONE_LINK, TEST_INSTANCE_ID));
  }

  @Test
  public void testParseResourceDataParsesSelfLink() {
    Optional<InstanceResourceData> result =
        ComputeClient.parseInstanceResourceData(
            "https://www.googleapis.com/compute/v1/projects/test-project-1/zones/test-zone-1/instances/test-name");
    assertTrue(result.isPresent());
    assertEquals("test-project-1", result.get().getProjectId());
    assertEquals("test-zone-1", result.get().getZone());
    assertEquals("test-name", result.get().getName());
  }

  @Test
  public void testParseResourceDataReturnsEmptyWithInvalidInput() {
    Optional<InstanceResourceData> result = ComputeClient.parseInstanceResourceData("fizz-buzz");
    assertFalse(result.isPresent());
  }
}
