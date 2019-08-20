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

package com.google.graphite.platforms.plugin.client;

import static com.google.graphite.platforms.plugin.client.util.ClientUtil.processResourceList;

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
}
