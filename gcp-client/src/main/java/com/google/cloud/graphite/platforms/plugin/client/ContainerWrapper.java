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

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.services.container.Container;
import com.google.api.services.container.model.Cluster;
import java.io.IOException;
import java.util.List;

/*
 * Internal use only. Wraps chained methods with direct calls for readability and mocking. The lack
 * of final parameters or parameter checking is intended, this is purely for wrapping.
 */
class ContainerWrapper {
  private static final String GET_RESOURCE_URL = "https://%s/v2/%s/%s/manifests/%s";
  private static final String MANIFEST_ACCEPT_TYPE =
      "application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json";
  private static final String DIGEST_HEADER = "docker-content-digest";
  private final Container container;

  ContainerWrapper(Container container) {
    this.container = container;
  }

  Cluster getCluster(String projectId, String location, String clusterName) throws IOException {
    return container
        .projects()
        .locations()
        .clusters()
        .get(toApiName(projectId, location, clusterName))
        .execute();
  }

  List<Cluster> listClusters(String projectId, String location) throws IOException {
    return container
        .projects()
        .locations()
        .clusters()
        .list(toApiParent(projectId, location))
        .execute()
        .getClusters();
  }

  String getDigest(String domain, String projectId, String name, String tag) throws IOException {
    HttpRequest request =
        container
            .getRequestFactory()
            .buildGetRequest(
                new GenericUrl(String.format(GET_RESOURCE_URL, domain, projectId, name, tag)));
    HttpHeaders headers = request.getHeaders();
    headers.setAccept(MANIFEST_ACCEPT_TYPE);
    return request
        .setHeaders(headers)
        .execute()
        .getHeaders()
        .getFirstHeaderStringValue(DIGEST_HEADER);
  }

  private static String toApiName(
      final String projectId, final String location, final String clusterName) {
    return String.format("projects/%s/locations/%s/clusters/%s", projectId, location, clusterName);
  }

  private static String toApiParent(final String projectId, final String location) {
    return String.format("projects/%s/locations/%s", projectId, location);
  }
}
