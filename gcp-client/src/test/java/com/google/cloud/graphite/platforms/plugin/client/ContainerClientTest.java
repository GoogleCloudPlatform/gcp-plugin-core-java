/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.graphite.platforms.plugin.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;

import com.google.api.services.container.model.Cluster;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests {@link com.google.cloud.graphite.platforms.plugin.client.ContainerClient}. */
@RunWith(MockitoJUnitRunner.class)
public class ContainerClientTest {
  private static final String TEST_PROJECT_ID = "test-project-id";
  private static final String TEST_LOCATION = "us-west1-a";
  private static final String TEST_CLUSTER = "testCluster";
  private static final String OTHER_CLUSTER = "otherCluster";

  @Test(expected = IllegalArgumentException.class)
  public void testGetClustersErrorWithNullProjectId() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    ContainerClient containerClient = new ContainerClient(container);
    containerClient.getCluster(null, TEST_LOCATION, TEST_CLUSTER);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetClustersErrorWithNullLocation() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    ContainerClient containerClient = new ContainerClient(container);
    containerClient.getCluster(TEST_PROJECT_ID, null, TEST_CLUSTER);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetClustersErrorWithNullClusterName() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    ContainerClient containerClient = new ContainerClient(container);
    containerClient.getCluster(TEST_PROJECT_ID, TEST_LOCATION, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetClustersErrorWithEmptyProjectId() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    ContainerClient containerClient = new ContainerClient(container);
    containerClient.getCluster("", TEST_LOCATION, TEST_CLUSTER);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetClustersErrorWithEmptyLocation() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    ContainerClient containerClient = new ContainerClient(container);
    containerClient.getCluster(TEST_PROJECT_ID, "", TEST_CLUSTER);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetClustersErrorWithEmptyClusterName() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    ContainerClient containerClient = new ContainerClient(container);
    containerClient.getCluster(TEST_PROJECT_ID, TEST_LOCATION, "");
  }

  @Test
  public void testGetClusterReturnsProperlyWhenClusterExists() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    Cluster expected = new Cluster().setName(TEST_CLUSTER);
    Mockito.when(container.getCluster(anyString(), anyString(), anyString())).thenReturn(expected);
    ContainerClient containerClient = new ContainerClient(container);
    Cluster response = containerClient.getCluster(TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER);
    assertNotNull(response);
    assertEquals(expected, response);
  }

  @Test(expected = IOException.class)
  public void testGetClusterThrowsErrorWhenClusterDoesntExists() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    Mockito.when(container.getCluster(anyString(), anyString(), anyString()))
        .thenThrow(IOException.class);
    ContainerClient containerClient = new ContainerClient(container);
    containerClient.getCluster(TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListAllClustersErrorWithNullProjectId() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    ContainerClient containerClient = new ContainerClient(container);
    containerClient.listAllClusters(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListAllClustersErrorWithEmptyProjectId() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    ContainerClient containerClient = new ContainerClient(container);
    containerClient.listAllClusters("");
  }

  @Test
  public void testListAllClustersWithValidInputsWhenClustersExist() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    Mockito.when(container.listClusters(anyString(), anyString()))
        .thenReturn(initClusterList(ImmutableList.of(TEST_CLUSTER)));
    ContainerClient containerClient = new ContainerClient(container);
    List<Cluster> expected = initClusterList(ImmutableList.of(TEST_CLUSTER));
    List<Cluster> response = containerClient.listAllClusters(TEST_PROJECT_ID);
    assertNotNull(response);
    assertEquals(expected, response);
  }

  @Test
  public void testListAllClustersSortedWithMultipleClusters() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    Mockito.when(container.listClusters(anyString(), anyString()))
        .thenReturn(initClusterList(ImmutableList.of(TEST_CLUSTER, OTHER_CLUSTER)));
    ContainerClient containerClient = new ContainerClient(container);
    List<Cluster> expected = initClusterList(ImmutableList.of(OTHER_CLUSTER, TEST_CLUSTER));
    List<Cluster> response = containerClient.listAllClusters(TEST_PROJECT_ID);
    assertNotNull(response);
    assertEquals(expected, response);
  }

  @Test
  public void testListAllClustersWithValidInputsWhenClustersIsNull() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    Mockito.when(container.listClusters(anyString(), anyString())).thenReturn(null);
    ContainerClient containerClient = new ContainerClient(container);
    List<Cluster> expected = ImmutableList.of();
    List<Cluster> response = containerClient.listAllClusters(TEST_PROJECT_ID);
    assertNotNull(response);
    assertEquals(expected, response);
  }

  @Test
  public void testListAllClustersEmptyWithValidProjectWithNoClusters() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    Mockito.when(container.listClusters(anyString(), anyString())).thenReturn(ImmutableList.of());
    ContainerClient containerClient = new ContainerClient(container);
    List<Cluster> expected = ImmutableList.of();
    List<Cluster> response = containerClient.listAllClusters(TEST_PROJECT_ID);
    assertNotNull(response);
    assertEquals(expected, response);
  }

  @Test(expected = IOException.class)
  public void testListAllClustersThrowsErrorWithInvalidProject() throws IOException {
    ContainerWrapper container = Mockito.mock(ContainerWrapper.class);
    Mockito.when(container.listClusters(anyString(), anyString())).thenThrow(IOException.class);
    ContainerClient containerClient = new ContainerClient(container);
    containerClient.listAllClusters(TEST_PROJECT_ID);
  }

  private static List<Cluster> initClusterList(List<String> clusterNames) {
    if (clusterNames == null) {
      return null;
    }
    List<Cluster> clusters = new ArrayList<>();
    clusterNames.forEach(e -> clusters.add(new Cluster().setName(e).setLocation(TEST_LOCATION)));
    return clusters;
  }
}
