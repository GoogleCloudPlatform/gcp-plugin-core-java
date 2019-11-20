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
import static org.mockito.ArgumentMatchers.anyString;

import com.google.api.services.binaryauthorization.v1beta1.model.Attestor;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests {@link com.google.cloud.graphite.platforms.plugin.client.BinaryAuthorizationClient}. */
@RunWith(MockitoJUnitRunner.class)
public class BinaryAuthorizationClientTest {
  private static final String TEST_PROJECT_ID = "test-project-id";
  private static final String TEST_ATTESTOR = "test-attestor";
  private static final String OTHER_ATTESTOR = "other-attestor";

  @Test(expected = IllegalArgumentException.class)
  public void testGetAttestorErrorWithNullProjectId() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    binaryAuthorizationClient.getAttestor(null, TEST_ATTESTOR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetAttestorErrorWithNullAttestor() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    binaryAuthorizationClient.getAttestor(TEST_PROJECT_ID, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetAttestorErrorWithEmptyProjectId() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    binaryAuthorizationClient.getAttestor("", TEST_ATTESTOR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetAttestorErrorWithEmptyAttestor() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    binaryAuthorizationClient.getAttestor(TEST_PROJECT_ID, "");
  }

  @Test
  public void testGetAttestorReturnsProperlyWhenAttestorExists() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    Attestor expected = new Attestor().setName(TEST_ATTESTOR);
    Mockito.when(binaryAuthorization.getAttestor(anyString(), anyString())).thenReturn(expected);
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    Attestor response = binaryAuthorizationClient.getAttestor(TEST_PROJECT_ID, TEST_ATTESTOR);
    assertNotNull(response);
    assertEquals(expected, response);
  }

  @Test(expected = IOException.class)
  public void testGetAttestorThrowsErrorWhenAttestorDoesntExist() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    Mockito.when(binaryAuthorization.getAttestor(anyString(), anyString()))
        .thenThrow(IOException.class);
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    binaryAuthorizationClient.getAttestor(TEST_PROJECT_ID, OTHER_ATTESTOR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListAttestorsErrorWithNullProjectId() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    binaryAuthorizationClient.listAttestors(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListAttestorsErrorWithEmptyProjectId() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    binaryAuthorizationClient.listAttestors("");
  }

  @Test
  public void testListAttestorsWithValidInputsWhenAttestorExists() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    Mockito.when(binaryAuthorization.listAttestors(anyString()))
        .thenReturn(initAttestorList(ImmutableList.of(TEST_ATTESTOR)));
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    List<Attestor> expected = initAttestorList(ImmutableList.of(TEST_ATTESTOR));
    List<Attestor> response = binaryAuthorizationClient.listAttestors(TEST_PROJECT_ID);
    assertNotNull(response);
    assertEquals(expected, response);
  }

  @Test
  public void testListAttestorsWithValidInputsWithMultipleAttestors() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    Mockito.when(binaryAuthorization.listAttestors(anyString()))
        .thenReturn(initAttestorList(ImmutableList.of(TEST_ATTESTOR, OTHER_ATTESTOR)));
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    List<Attestor> expected = initAttestorList(ImmutableList.of(OTHER_ATTESTOR, TEST_ATTESTOR));
    List<Attestor> response = binaryAuthorizationClient.listAttestors(TEST_PROJECT_ID);
    assertNotNull(response);
    assertEquals(expected, response);
  }

  @Test
  public void testListAttestorsWithValidInputsWhenAttestorsIsNull() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    Mockito.when(binaryAuthorization.listAttestors(anyString())).thenReturn(null);
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    List<Attestor> expected = ImmutableList.of();
    List<Attestor> response = binaryAuthorizationClient.listAttestors(TEST_PROJECT_ID);
    assertNotNull(response);
    assertEquals(expected, response);
  }

  @Test
  public void testListAttestorsWithValidProjectWithNoAttestors() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    Mockito.when(binaryAuthorization.listAttestors(anyString())).thenReturn(ImmutableList.of());
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    List<Attestor> expected = ImmutableList.of();
    List<Attestor> response = binaryAuthorizationClient.listAttestors(TEST_PROJECT_ID);
    assertNotNull(response);
    assertEquals(expected, response);
  }

  @Test(expected = IOException.class)
  public void testListAttestorsThrowsErrorWithInvalidProject() throws IOException {
    BinaryAuthorizationWrapper binaryAuthorization = Mockito.mock(BinaryAuthorizationWrapper.class);
    Mockito.when(binaryAuthorization.listAttestors(anyString())).thenThrow(IOException.class);
    BinaryAuthorizationClient binaryAuthorizationClient =
        new BinaryAuthorizationClient(binaryAuthorization);
    binaryAuthorizationClient.listAttestors(TEST_PROJECT_ID);
  }

  private static List<Attestor> initAttestorList(List<String> attestorNames) {
    if (attestorNames == null) {
      return null;
    }
    List<Attestor> attestors = new ArrayList<>();
    attestorNames.forEach(a -> attestors.add(new Attestor().setName(a)));
    return attestors;
  }
}
