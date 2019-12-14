/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.graphite.platforms.plugin.client;

import static com.google.cloud.graphite.platforms.plugin.client.util.ClientUtil.processResourceList;

import com.google.api.services.container.Container;
import com.google.api.services.container.model.Cluster;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Comparator;

/**
 * Client for communicating with the Google GKE API.
 *
 * @see <a href="https://cloud.google.com/kubernetes-engine/docs/reference/rest/">Kubernetes
 *     Engine</a>
 */
public class ContainerClient {
  private static final String LOCATION_WILDCARD = "-";
  private final ContainerWrapper container;

  /**
   * Constructs a new {@link ContainerClient} instance.
   *
   * @param container The {@link Container} instance this class will utilize for interacting with
   *     the GKE API.
   */
  public ContainerClient(final ContainerWrapper container) {
    this.container = Preconditions.checkNotNull(container);
  }

  /**
   * Retrieves a {@link Cluster} from the container client.
   *
   * @param projectId The ID of the project the cluster resides in.
   * @param location The location of the cluster.
   * @param cluster The name of the cluster.
   * @return The retrieved {@link Cluster}.
   * @throws IOException When an error occurred attempting to get the cluster.
   */
  public Cluster getCluster(final String projectId, final String location, final String cluster)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(location));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cluster));
    return container.getCluster(projectId, location, cluster);
  }

  /**
   * Retrieves a list of all {@link Cluster} objects for the project from the container client.
   *
   * @param projectId The ID of the project the clusters reside in.
   * @return The retrieved list of {@link Cluster}s sorted by name.
   * @throws IOException When an error occurred attempting to get the list of clusters.
   */
  public ImmutableList<Cluster> listAllClusters(final String projectId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    return processResourceList(
        container.listClusters(projectId, LOCATION_WILDCARD),
        Comparator.comparing(Cluster::getName));
  }

  /**
   * Retrieves the sha digest of the provided container resource URI and tag
   *
   * @param resourceUri The URI of the container image, such as "gcr.io/example-project/example".
   * @param tag A container image tag such as "latest".
   * @return The container digest, such as
   *     sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef The container can
   *     be specified either by "gcr.io/example-project/example:latest" or
   *     "gcr.io/example-project/example@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
   * @throws IOException If there was an error retrieving the digest
   */
  public String getDigest(final String resourceUri, final String tag) throws IOException {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(resourceUri) && resourceUri.split("/").length >= 3);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tag));
    String[] tokens = resourceUri.split("/", 3);
    return container.getDigest(tokens[0], tokens[1], tokens[2], tag);
  }
}
