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

import static com.google.cloud.graphite.platforms.plugin.client.ContainerAnalysisWrapper.toAttestationOccurrence;
import static com.google.cloud.graphite.platforms.plugin.client.ContainerAnalysisWrapper.toOccurrenceName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;

import com.google.api.services.containeranalysis.v1beta1.model.Discovered;
import com.google.api.services.containeranalysis.v1beta1.model.GrafeasV1beta1DiscoveryDetails;
import com.google.api.services.containeranalysis.v1beta1.model.GrafeasV1beta1VulnerabilityDetails;
import com.google.api.services.containeranalysis.v1beta1.model.Occurrence;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests {@link ContainerAnalysisClient}. */
@RunWith(MockitoJUnitRunner.class)
public class ContainerAnalysisClientTest {
  private static final String TEST_PROJECT_ID = "test-project";
  private static final String TEST_RESOURCE_URL = "gcr.io/test-project/test@sha256:abcdef";
  private static final String TEST_OCCURRENCE_ID = "test-occurrence";
  private static final String OTHER_OCCURRENCE_ID = "other-occurrence";
  private static final String TEST_NOTE_PROJECT_ID = "test-note-project";
  private static final String TEST_NOTE_ID = "test-note";
  private static final String TEST_SIGNATURE = "test-signature";
  private static final String TEST_PUBLIC_KEY_ID = "test-public-key";
  private static final String TEST_PAYLOAD = "test-payload";

  @Test(expected = IllegalArgumentException.class)
  public void testGetVulnerabilityScanStatusSyncErrorWithNullProjectId() {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.getVulnerabilityScanStatusSync(null, TEST_RESOURCE_URL, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetVulnerabilityScanStatusSyncErrorWithEmptyProjectId() {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.getVulnerabilityScanStatusSync("", TEST_RESOURCE_URL, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetVulnerabilityScanStatusSyncErrorWithNullResourceUrl() {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.getVulnerabilityScanStatusSync(TEST_PROJECT_ID, null, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetVulnerabilityScanStatusSyncErrorWithEmptyResourceUrl() {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.getVulnerabilityScanStatusSync(TEST_PROJECT_ID, "", 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetVulnerabilityScanStatusSyncErrorWithNegativeTimeout() {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.getVulnerabilityScanStatusSync(TEST_PROJECT_ID, TEST_RESOURCE_URL, -10);
  }

  @Test
  public void testGetVulnerabilityScanStatusSync() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    Mockito.when(containerAnalysis.listOccurrences(anyString(), anyString()))
        .thenAnswer(
            invocationOnMock -> {
              Thread.sleep(500);
              return ImmutableList.of(getDiscoveryOccurrence("PENDING"));
            });
    Iterator<String> statuses =
        ImmutableList.of("SCANNING", "SCANNING", "FINISHED_SUCCESS").iterator();
    Mockito.when(containerAnalysis.getOccurrence(anyString(), anyString()))
        .thenAnswer(
            invocationOnMock -> {
              Thread.sleep(500);
              return getDiscoveryOccurrence(statuses.next());
            });
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    String result =
        containerAnalysisClient.getVulnerabilityScanStatusSync(
            TEST_PROJECT_ID, TEST_RESOURCE_URL, 7000);
    assertEquals("FINISHED_SUCCESS", result);
  }

  @Test
  public void testGetVulnerabilityStatusSyncWithOtherStatus() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    Mockito.when(containerAnalysis.listOccurrences(anyString(), anyString()))
        .thenAnswer(
            invocationOnMock -> {
              Thread.sleep(500);
              return ImmutableList.of(getDiscoveryOccurrence("PENDING"));
            });
    Mockito.when(containerAnalysis.getOccurrence(anyString(), anyString()))
        .thenAnswer(
            invocationOnMock -> {
              Thread.sleep(500);
              return getDiscoveryOccurrence("FINISHED_UNSUPPORTED");
            });
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    String result =
        containerAnalysisClient.getVulnerabilityScanStatusSync(
            TEST_PROJECT_ID, TEST_RESOURCE_URL, 7000);
    assertEquals("FINISHED_UNSUPPORTED", result);
  }

  @Test(expected = ConditionTimeoutException.class)
  public void testGetVulnerabilityScanStatusSyncTimeoutWithNoDiscoveryOccurrence()
      throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    Mockito.when(containerAnalysis.listOccurrences(anyString(), anyString()))
        .thenAnswer(
            invocationOnMock -> {
              Thread.sleep(500);
              return ImmutableList.of();
            });
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.getVulnerabilityScanStatusSync(
        TEST_PROJECT_ID, TEST_RESOURCE_URL, 2000);
  }

  @Test(expected = ConditionTimeoutException.class)
  public void testGetVulnerabilityScanStatusSyncTimeoutWithNoFinishedStatus() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    Mockito.when(containerAnalysis.listOccurrences(anyString(), anyString()))
        .thenAnswer(
            invocationOnMock -> {
              Thread.sleep(500);
              return ImmutableList.of(getDiscoveryOccurrence("PENDING"));
            });
    Mockito.when(containerAnalysis.getOccurrence(anyString(), anyString()))
        .thenAnswer(
            invocationOnMock -> {
              Thread.sleep(500);
              return getDiscoveryOccurrence("PENDING");
            });
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.getVulnerabilityScanStatusSync(
        TEST_PROJECT_ID, TEST_RESOURCE_URL, 7000);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListVulnerabilityScanOccurrencesErrorWithNullProjectId() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.listVulnerabilityScanOccurrences(null, TEST_RESOURCE_URL);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListVulnerabilityScanOccurrencesErrorWithEmptyProjectId() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.listVulnerabilityScanOccurrences("", TEST_RESOURCE_URL);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListVulnerabilityScanOccurrencesErrorWithNullResourceUrl() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.listVulnerabilityScanOccurrences(TEST_PROJECT_ID, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListVulnerabilityScanOccurrencesErrorWithEmptyResourceUrl() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.listVulnerabilityScanOccurrences(TEST_PROJECT_ID, "");
  }

  @Test
  public void testListVulnerabilityScanOccurrences() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    String expectedFilter =
        String.format("resourceUrl=\"%s\" AND kind=\"VULNERABILITY\"", TEST_RESOURCE_URL);
    Mockito.when(containerAnalysis.listOccurrences(TEST_PROJECT_ID, expectedFilter))
        .thenReturn(
            ImmutableList.of(
                getVulnerabilityOccurrence(TEST_OCCURRENCE_ID, "High"),
                getVulnerabilityOccurrence(OTHER_OCCURRENCE_ID, "Low")));
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    List<Occurrence> expected =
        ImmutableList.of(
            getVulnerabilityOccurrence(OTHER_OCCURRENCE_ID, "Low"),
            getVulnerabilityOccurrence(TEST_OCCURRENCE_ID, "High"));
    List<Occurrence> result =
        containerAnalysisClient.listVulnerabilityScanOccurrences(
            TEST_PROJECT_ID, TEST_RESOURCE_URL);
    assertNotNull(result);
    assertEquals(expected, result);
  }

  @Test(expected = IOException.class)
  public void testListVulnerabilityScanOccurrencesErrorWithIOException() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    Mockito.when(containerAnalysis.listOccurrences(anyString(), anyString()))
        .thenThrow(IOException.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.listVulnerabilityScanOccurrences(TEST_PROJECT_ID, TEST_RESOURCE_URL);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithNullProjectId() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        null,
        TEST_RESOURCE_URL,
        TEST_NOTE_PROJECT_ID,
        TEST_NOTE_ID,
        TEST_SIGNATURE,
        TEST_PUBLIC_KEY_ID,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithEmptyProjectId() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        "",
        TEST_RESOURCE_URL,
        TEST_NOTE_PROJECT_ID,
        TEST_NOTE_ID,
        TEST_SIGNATURE,
        TEST_PUBLIC_KEY_ID,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithNullResourceUrl() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        null,
        TEST_NOTE_PROJECT_ID,
        TEST_NOTE_ID,
        TEST_SIGNATURE,
        TEST_PUBLIC_KEY_ID,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithEmptyResourceUrl() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        "",
        TEST_NOTE_PROJECT_ID,
        TEST_NOTE_ID,
        TEST_SIGNATURE,
        TEST_PUBLIC_KEY_ID,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithNullNoteProjectId() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        TEST_RESOURCE_URL,
        null,
        TEST_NOTE_ID,
        TEST_SIGNATURE,
        TEST_PUBLIC_KEY_ID,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithEmptyNoteProjectId() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        TEST_RESOURCE_URL,
        "",
        TEST_NOTE_ID,
        TEST_SIGNATURE,
        TEST_PUBLIC_KEY_ID,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithNullNoteId() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        TEST_RESOURCE_URL,
        TEST_NOTE_PROJECT_ID,
        null,
        TEST_SIGNATURE,
        TEST_PUBLIC_KEY_ID,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithEmptyNoteId() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        TEST_RESOURCE_URL,
        TEST_NOTE_PROJECT_ID,
        "",
        TEST_SIGNATURE,
        TEST_PUBLIC_KEY_ID,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithNullSignature() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        TEST_RESOURCE_URL,
        TEST_NOTE_PROJECT_ID,
        TEST_NOTE_ID,
        null,
        TEST_PUBLIC_KEY_ID,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithEmptySignature() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        TEST_RESOURCE_URL,
        TEST_NOTE_PROJECT_ID,
        TEST_NOTE_ID,
        "",
        TEST_PUBLIC_KEY_ID,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithNullPublicKeyId() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        TEST_RESOURCE_URL,
        TEST_NOTE_PROJECT_ID,
        TEST_NOTE_ID,
        TEST_SIGNATURE,
        null,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithEmptyPublicKeyId() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        TEST_RESOURCE_URL,
        TEST_NOTE_PROJECT_ID,
        TEST_NOTE_ID,
        TEST_SIGNATURE,
        "",
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithNullPayload() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        TEST_RESOURCE_URL,
        TEST_NOTE_PROJECT_ID,
        TEST_NOTE_ID,
        TEST_SIGNATURE,
        TEST_PUBLIC_KEY_ID,
        null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAttestationErrorWithEmptyPayload() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        TEST_RESOURCE_URL,
        TEST_NOTE_PROJECT_ID,
        TEST_NOTE_ID,
        TEST_SIGNATURE,
        TEST_PUBLIC_KEY_ID,
        "");
  }

  @Test
  public void testCreateAttestation() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    Occurrence expected =
        toAttestationOccurrence(
            TEST_RESOURCE_URL,
            TEST_NOTE_PROJECT_ID,
            TEST_NOTE_ID,
            TEST_SIGNATURE,
            TEST_PUBLIC_KEY_ID,
            TEST_PAYLOAD);
    Mockito.when(
            containerAnalysis.createAttestation(
                TEST_PROJECT_ID,
                TEST_RESOURCE_URL,
                TEST_NOTE_PROJECT_ID,
                TEST_NOTE_ID,
                TEST_SIGNATURE,
                TEST_PUBLIC_KEY_ID,
                TEST_PAYLOAD))
        .thenReturn(expected);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    Occurrence result =
        containerAnalysisClient.createAttestation(
            TEST_PROJECT_ID,
            TEST_RESOURCE_URL,
            TEST_NOTE_PROJECT_ID,
            TEST_NOTE_ID,
            TEST_SIGNATURE,
            TEST_PUBLIC_KEY_ID,
            TEST_PAYLOAD);
    assertNotNull(result);
    assertEquals(expected, result);
  }

  @Test(expected = IOException.class)
  public void testCreateAttestationErrorWithIOException() throws IOException {
    ContainerAnalysisWrapper containerAnalysis = Mockito.mock(ContainerAnalysisWrapper.class);
    Mockito.when(
            containerAnalysis.createAttestation(
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyString()))
        .thenThrow(IOException.class);
    ContainerAnalysisClient containerAnalysisClient =
        new ContainerAnalysisClient(containerAnalysis);
    containerAnalysisClient.createAttestation(
        TEST_PROJECT_ID,
        TEST_RESOURCE_URL,
        TEST_NOTE_PROJECT_ID,
        TEST_NOTE_ID,
        TEST_SIGNATURE,
        TEST_PUBLIC_KEY_ID,
        TEST_PAYLOAD);
  }

  private static Occurrence getDiscoveryOccurrence(String status) {
    return new Occurrence()
        .setName(toOccurrenceName(TEST_PROJECT_ID, TEST_OCCURRENCE_ID))
        .setKind("DISCOVERY")
        .setDiscovered(
            new GrafeasV1beta1DiscoveryDetails()
                .setDiscovered(new Discovered().setAnalysisStatus(status)));
  }

  private static Occurrence getVulnerabilityOccurrence(String occurrenceId, String severity) {
    return new Occurrence()
        .setName(toOccurrenceName(TEST_PROJECT_ID, occurrenceId))
        .setKind("VULNERABILITY")
        .setVulnerability(new GrafeasV1beta1VulnerabilityDetails().setSeverity(severity));
  }
}
