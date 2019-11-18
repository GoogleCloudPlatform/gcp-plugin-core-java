package com.google.cloud.graphite.platforms.plugin.client;

import com.google.api.services.containeranalysis.v1beta1.ContainerAnalysis;
import com.google.api.services.containeranalysis.v1beta1.model.Attestation;
import com.google.api.services.containeranalysis.v1beta1.model.Details;
import com.google.api.services.containeranalysis.v1beta1.model.GenericSignedAttestation;
import com.google.api.services.containeranalysis.v1beta1.model.Occurrence;
import com.google.api.services.containeranalysis.v1beta1.model.Resource;
import com.google.api.services.containeranalysis.v1beta1.model.Signature;
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
        .get(toOccurenceName(projectId, occurrenceId))
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

  private static String toOccurenceName(String projectId, String occurrenceId) {
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

  private static Occurrence toAttestationOccurrence(
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
