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

import static com.google.cloud.graphite.platforms.plugin.client.util.ClientUtil.buildFilterString;
import static com.google.cloud.graphite.platforms.plugin.client.util.ClientUtil.nameFromSelfLink;
import static com.google.cloud.graphite.platforms.plugin.client.util.ClientUtil.processResourceList;

import com.google.api.services.containeranalysis.v1beta1.model.Occurrence;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.java.Log;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

/**
 * Client for communicating with the Container Analysis API.
 *
 * @see <a href="https://cloud.google.com/container-registry/docs/container-analysis">Container
 *     Analysis</a>
 */
@Log
public class ContainerAnalysisClient {
  private static final String VULNERABILITY_NOTE_PROJECT_ID = "goog-analysis";
  private static final String VULNERABILITY_NOTE_ID = "PACKAGE_VULNERABILITY";
  private static final String VULNERABILITY_KIND = "VULNERABILITY";
  private static final List<String> FINISHED_STATUSES =
      ImmutableList.of("FINISHED_SUCCESS", "FINISHED_FAILED", "FINISHED_UNSUPPORTED");
  private ContainerAnalysisWrapper containerAnalysis;

  ContainerAnalysisClient(ContainerAnalysisWrapper containerAnalysis) {
    this.containerAnalysis = containerAnalysis;
  }

  /**
   * Wait until the vulnerability scan of the provided container image is complete and return the
   * final status.
   *
   * @param projectId The ID of the project where the container image is hosted.
   * @param resourceUrl The complete URL of the image in this form:
   *     https://[HOST_NAME]/[PROJECT_ID]/[IMAGE_ID]@sha256:[HASH]
   * @param timeoutMillis The number of milliseconds to wait for the scan before timing out.
   * @return The final status of the scan, one of FINISHED_SUCCESS, FINISHED_FAILED, or
   *     FINISHED_UNSUPPORTED.
   * @throws ConditionTimeoutException If the the analysis was not in a finished state before the
   *     timeout expired.
   */
  public String getVulnerabilityScanStatusSync(
      final String projectId, final String resourceUrl, final long timeoutMillis)
      throws ConditionTimeoutException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(resourceUrl));
    Preconditions.checkArgument(timeoutMillis >= 0);
    long start = System.currentTimeMillis();
    Occurrence statusOccurence =
        Awaitility.await()
            .timeout(timeoutMillis, TimeUnit.MILLISECONDS)
            .until(() -> getStatusOccurrence(projectId, resourceUrl), Optional::isPresent)
            .get();
    long timeLeft = timeoutMillis - (System.currentTimeMillis() - start);
    return Awaitility.await()
        .timeout(timeLeft, TimeUnit.MILLISECONDS)
        .until(
            () ->
                getVulnerabilityScanStatus(projectId, nameFromSelfLink(statusOccurence.getName())),
            Optional::isPresent)
        .get();
  }

  /**
   * Return a list of occurrences related to a vulnerability on the provided image.
   *
   * @param projectId The ID of the project where the container image is hosted.
   * @param resourceUrl The complete URL of the image in this form:
   *     https://[HOST_NAME]/[PROJECT_ID]/[IMAGE_ID]@sha256:[HASH]
   * @return A list of {@link Occurrence}s of vulnerabilities on the image, sorted by name.
   * @throws IOException An error occurred attempting to get the list of occurrences.
   */
  public ImmutableList<Occurrence> listVulnerabilityScanOccurrences(
      final String projectId, final String resourceUrl) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(resourceUrl));
    return processResourceList(
        containerAnalysis.listOccurrences(
            projectId,
            buildFilterString(
                ImmutableMap.of("resourceUrl", resourceUrl, "kind", VULNERABILITY_KIND))),
        Comparator.comparing(Occurrence::getName));
  }

  /**
   * @param projectId The project ID where the container image is hosted.
   * @param resourceUrl The complete URL of the image in this form:
   *     https://[HOST_NAME]/[PROJECT_ID]/[IMAGE_ID]@sha256:[HASH]
   * @param noteProjectId The project ID of the note representing the attestation authority.
   * @param noteId The note ID of the note representing the attestation authority.
   * @param signature The signed payload created using the key referenced by the publicKeyId.
   * @param publicKeyId The id of the public key for the asymmetric PIKIX key used to sign the
   *     payload and produce the signature.
   * @param payload The base64-encoded attestation verified by the provided signature.
   * @return The attestation {@link Occurrence} that was created.
   * @throws IOException An error occurred attempting to get the occurrence.
   */
  public Occurrence createAttestation(
      final String projectId,
      final String resourceUrl,
      final String noteProjectId,
      final String noteId,
      final String signature,
      final String publicKeyId,
      final String payload)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(resourceUrl));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(noteProjectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(noteId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(signature));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(payload));
    return containerAnalysis.createAttestation(
        projectId, resourceUrl, noteProjectId, noteId, signature, publicKeyId, payload);
  }

  private Optional<Occurrence> getStatusOccurrence(String projectId, String resourceUrl) {
    List<Occurrence> occurrences;
    try {
      occurrences =
          containerAnalysis.listOccurrences(
              projectId,
              buildFilterString(
                  ImmutableMap.of(
                      "resourceUrl",
                      resourceUrl,
                      "noteProjectId",
                      VULNERABILITY_NOTE_PROJECT_ID,
                      "noteId",
                      VULNERABILITY_NOTE_ID)));
    } catch (IOException ioe) {
      log.info(String.format("Error listing occurrences: %s. Retrying ...", ioe.getMessage()));
      return Optional.empty();
    }
    for (Occurrence o : occurrences) {
      if (o.getDiscovered() != null) {
        return Optional.of(o);
      }
    }
    log.info("Did not find occurrences.");
    return Optional.empty();
  }

  private Optional<String> getVulnerabilityScanStatus(String projectId, String occurrenceId) {
    Occurrence occurrence;
    try {
      occurrence = containerAnalysis.getOccurrence(projectId, occurrenceId);
    } catch (IOException ioe) {
      log.info(
          String.format(
              "Error retrieving vulnerability status occurrence projects/%s/occurrences/%s: %s",
              projectId, occurrenceId, ioe.getMessage()));
      return Optional.empty();
    }
    String status = occurrence.getDiscovered().getDiscovered().getAnalysisStatus();
    if (FINISHED_STATUSES.contains(status)) {
      return Optional.of(status);
    }
    log.info(String.format("Vulnerability scan is not finished. Current status is %s.", status));
    return Optional.empty();
  }
}
