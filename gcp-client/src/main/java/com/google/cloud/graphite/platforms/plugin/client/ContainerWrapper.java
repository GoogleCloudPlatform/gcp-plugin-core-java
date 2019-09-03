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

import com.google.api.services.container.Container;
import com.google.api.services.container.model.Cluster;
import java.io.IOException;
import java.util.List;

/*
 * Internal use only. Wraps chained methods with direct calls for readability and mocking. The lack
 * of final parameters or parameter checking is intended, this is purely for wrapping.
 */
class ContainerWrapper {
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

  private static String toApiName(
      final String projectId, final String location, final String clusterName) {
    return String.format("projects/%s/locations/%s/clusters/%s", projectId, location, clusterName);
  }

  private static String toApiParent(final String projectId, final String location) {
    return String.format("projects/%s/locations/%s", projectId, location);
  }
}
