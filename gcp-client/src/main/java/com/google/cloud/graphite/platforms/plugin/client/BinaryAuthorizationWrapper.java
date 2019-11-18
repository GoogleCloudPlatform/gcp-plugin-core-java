package com.google.cloud.graphite.platforms.plugin.client;

import com.google.api.services.binaryauthorization.v1beta1.BinaryAuthorization;
import com.google.api.services.binaryauthorization.v1beta1.model.Attestor;
import java.io.IOException;
import java.util.List;

class BinaryAuthorizationWrapper {
  private BinaryAuthorization binaryAuthorization;

  BinaryAuthorizationWrapper(BinaryAuthorization binaryAuthorization) {
    this.binaryAuthorization = binaryAuthorization;
  }

  List<Attestor> listAttestors(String projectId) throws IOException {
    return binaryAuthorization
        .projects()
        .attestors()
        .list(toAttestorParent(projectId))
        .execute()
        .getAttestors();
  }

  Attestor getAttestor(String projectId, String attestor) throws IOException {
    return binaryAuthorization
        .projects()
        .attestors()
        .get(toAttestorName(projectId, attestor))
        .execute();
  }

  private static String toAttestorParent(final String projectId) {
    return String.format("projects/%s", projectId);
  }

  private static String toAttestorName(final String projectId, final String attestor) {
    return String.format("projects/%s/attestors/%s", projectId, attestor);
  }
}
