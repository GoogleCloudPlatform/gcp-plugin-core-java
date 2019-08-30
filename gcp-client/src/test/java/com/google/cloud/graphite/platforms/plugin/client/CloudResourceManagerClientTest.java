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

package com.google.cloud.graphite.platforms.plugin.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests {@link com.google.cloud.graphite.platforms.plugin.client.CloudResourceManagerClient}. */
@RunWith(MockitoJUnitRunner.class)
public class CloudResourceManagerClientTest {
  private static final List<String> TEST_PROJECT_IDS_SORTED =
      Arrays.asList("test-project-id-a1", "test-project-id-a2", "test-project-id-a3");
  private static final List<String> TEST_PROJECT_IDS_UNSORTED =
      Arrays.asList("test-project-id-b4", "test-project-id-b2", "test-project-id-b5");

  @Test(expected = IOException.class)
  public void testListProjectsErrorWithInvalidCredentials() throws IOException {
    CloudResourceManagerWrapper wrapper = Mockito.mock(CloudResourceManagerWrapper.class);
    Mockito.when(wrapper.listProjects()).thenThrow(IOException.class);
    CloudResourceManagerClient client = new CloudResourceManagerClient(wrapper);
    client.listProjects();
  }

  @Test
  public void testListProjectsNullReturnsEmpty() throws IOException {
    CloudResourceManagerWrapper wrapper = Mockito.mock(CloudResourceManagerWrapper.class);
    Mockito.when(wrapper.listProjects()).thenReturn(null);
    CloudResourceManagerClient client = new CloudResourceManagerClient(wrapper);
    List<Project> projects = client.listProjects();
    assertNotNull(projects);
    assertEquals(ImmutableList.of(), projects);
  }

  @Test
  public void testListProjectsEmptyReturnsEmpty() throws IOException {
    CloudResourceManagerWrapper wrapper = Mockito.mock(CloudResourceManagerWrapper.class);
    Mockito.when(wrapper.listProjects()).thenReturn(ImmutableList.of());
    CloudResourceManagerClient client = new CloudResourceManagerClient(wrapper);
    List<Project> projects = client.listProjects();
    assertNotNull(projects);
    assertEquals(ImmutableList.of(), projects);
  }

  @Test
  public void testListProjectsSorted() throws IOException {
    CloudResourceManagerWrapper wrapper = Mockito.mock(CloudResourceManagerWrapper.class);
    Mockito.when(wrapper.listProjects()).thenReturn(initProjectList(TEST_PROJECT_IDS_SORTED));
    CloudResourceManagerClient client = new CloudResourceManagerClient(wrapper);
    List<Project> projects = client.listProjects();
    assertNotNull(projects);
    assertEquals(initProjectList(TEST_PROJECT_IDS_SORTED), projects);
  }

  @Test
  public void testListProjectsUnsortedReturnedAsSorted() throws IOException {
    List<Project> expected = initProjectList(TEST_PROJECT_IDS_UNSORTED);
    expected.sort(Comparator.comparing(Project::getProjectId));
    CloudResourceManagerWrapper wrapper = Mockito.mock(CloudResourceManagerWrapper.class);
    Mockito.when(wrapper.listProjects()).thenReturn(initProjectList(TEST_PROJECT_IDS_UNSORTED));
    CloudResourceManagerClient client = new CloudResourceManagerClient(wrapper);
    List<Project> projects = client.listProjects();
    assertNotNull(projects);
    assertEquals(expected, projects);
  }

  private static List<Project> initProjectList(List<String> projectIds) {
    if (projectIds == null) {
      return null;
    }
    List<Project> projects = new ArrayList<>();
    projectIds.forEach(id -> projects.add(new Project().setProjectId(id)));
    return projects;
  }
}
