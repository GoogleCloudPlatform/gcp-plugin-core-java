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
