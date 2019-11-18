package com.google.cloud.graphite.platforms.plugin.client;

import com.google.api.services.binaryauthorization.v1beta1.model.Attestor;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;

public class BinaryAuthorizationClient {
  private BinaryAuthorizationWrapper binaryAuthorization;

  BinaryAuthorizationClient(BinaryAuthorizationWrapper binaryAuthorization) {
    this.binaryAuthorization = binaryAuthorization;
  }

  public ImmutableList<Attestor> listAttestors(final String projectId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    return ImmutableList.copyOf(binaryAuthorization.listAttestors(projectId));
  }

  public Attestor getAttestor(String projectId, String attestorName) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(attestorName));
    return binaryAuthorization.getAttestor(projectId, attestorName);
  }
}
