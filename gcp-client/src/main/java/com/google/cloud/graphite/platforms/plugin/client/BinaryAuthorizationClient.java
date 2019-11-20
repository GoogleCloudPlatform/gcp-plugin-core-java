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

import static com.google.cloud.graphite.platforms.plugin.client.util.ClientUtil.processResourceList;

import com.google.api.services.binaryauthorization.v1beta1.model.Attestor;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Comparator;

/**
 * Client for communicating with the Binary Authorization API.
 *
 * @see <a href="https://cloud.google.com/binary-authorization">Binary Authorization</a>
 */
public class BinaryAuthorizationClient {
  private BinaryAuthorizationWrapper binaryAuthorization;

  BinaryAuthorizationClient(BinaryAuthorizationWrapper binaryAuthorization) {
    this.binaryAuthorization = binaryAuthorization;
  }

  /**
   * Retrieves the list of available {@link Attestor}s in the specified project.
   *
   * @param projectId The ID of the project to check.
   * @return A list of {@link Attestor}s sorted by name.
   * @throws IOException An error occurred attempting to get the attestor.
   */
  public ImmutableList<Attestor> listAttestors(final String projectId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    return processResourceList(
        binaryAuthorization.listAttestors(projectId), Comparator.comparing(Attestor::getName));
  }

  /**
   * Retrieves the {@link Attestor} specified if it exists.
   *
   * @param projectId The ID of the project where the attestor is hosted.
   * @param attestorName The name of the attestor.
   * @return The {@link Attestor} specified by the provided project and name
   * @throws IOException An error occurred attempting to the the attestor.
   */
  public Attestor getAttestor(String projectId, String attestorName) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(attestorName));
    return binaryAuthorization.getAttestor(projectId, attestorName);
  }
}
