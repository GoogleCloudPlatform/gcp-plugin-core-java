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

import com.google.api.services.containeranalysis.v1beta1.ContainerAnalysis;
import com.google.api.services.containeranalysis.v1beta1.model.Attestation;
import com.google.api.services.containeranalysis.v1beta1.model.Details;
import com.google.api.services.containeranalysis.v1beta1.model.GenericSignedAttestation;
import com.google.api.services.containeranalysis.v1beta1.model.Occurrence;
import com.google.api.services.containeranalysis.v1beta1.model.Resource;
import com.google.api.services.containeranalysis.v1beta1.model.Signature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;

public class ContainerAnalysisWrapper {
  private ContainerAnalysis containerAnalysis;

  ContainerAnalysisWrapper(ContainerAnalysis containerAnalysis) {
    this.containerAnalysis = containerAnalysis;
  }

  List<Occurrence> listOccurrences(String projectId, String filter) throws IOException {
    return containerAnalysis
        .projects()
        .occurrences()
        .list(toOccurrenceParent(projectId))
        .setFilter(filter)
        .execute()
        .getOccurrences();
  }

  Occurrence getOccurrence(String projectId, String occurrenceId) throws IOException {
    return containerAnalysis
        .projects()
        .occurrences()
        .get(toOccurrenceName(projectId, occurrenceId))
        .execute();
  }

  Occurrence createAttestation(
      String projectId,
      String resourceUri,
      String noteProjectId,
      String noteId,
      String signature,
      String publicKeyId,
      String payload)
      throws IOException {
    return containerAnalysis
        .projects()
        .occurrences()
        .create(
            toOccurrenceParent(projectId),
            toAttestationOccurrence(
                resourceUri, noteProjectId, noteId, signature, publicKeyId, payload))
        .execute();
  }

  private static String toOccurrenceParent(String projectId) {
    return String.format("projects/%s", projectId);
  }

  @VisibleForTesting
  static String toOccurrenceName(String projectId, String occurrenceId) {
    return String.format("projects/%s/occurrences/%s", projectId, occurrenceId);
  }

  private static String toNoteName(String projectId, String note) {
    return String.format("projects/%s/note/%s", projectId, note);
  }

  private static Signature toSignature(String signature, String publicKeyId) {
    return new Signature().setSignature(signature).setPublicKeyId(publicKeyId);
  }

  private static Attestation toAttestation(String signature, String publicKeyId, String payload) {
    return new Attestation()
        .setGenericSignedAttestation(
            new GenericSignedAttestation()
                .setSignatures(ImmutableList.of(toSignature(signature, publicKeyId)))
                .setSerializedPayload(payload));
  }

  @VisibleForTesting
  static Occurrence toAttestationOccurrence(
      String resourceUri,
      String noteProjectId,
      String noteId,
      String signature,
      String publicKeyId,
      String payload) {
    return new Occurrence()
        .setResource(new Resource().setUri(resourceUri))
        .setNoteName(toNoteName(noteProjectId, noteId))
        .setKind("ATTESTATION")
        .setAttestation(
            new Details().setAttestation(toAttestation(signature, publicKeyId, payload)));
  }
}
