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

import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.AcceleratorType;
import com.google.api.services.compute.model.DiskType;
import com.google.api.services.compute.model.Image;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceTemplate;
import com.google.api.services.compute.model.InstancesScopedList;
import com.google.api.services.compute.model.MachineType;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.Network;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.Region;
import com.google.api.services.compute.model.Snapshot;
import com.google.api.services.compute.model.Subnetwork;
import com.google.api.services.compute.model.Zone;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/*
 * Internal use only. Wraps chained methods with direct calls for readability and mocking. The lack
 * of final parameters or parameter checking is intended, this is purely for wrapping.
 */
class ComputeWrapper {
  private final Compute compute;

  ComputeWrapper(Compute compute) {
    this.compute = compute;
  }

  List<Region> listRegions(String projectId) throws IOException {
    return compute.regions().list(projectId).execute().getItems();
  }

  Zone getZone(String projectId, String zone) throws IOException {
    return compute.zones().get(projectId, zone).execute();
  }

  List<Zone> listZones(String projectId) throws IOException {
    return compute.zones().list(projectId).execute().getItems();
  }

  List<MachineType> listMachineTypes(String projectId, String zone) throws IOException {
    return compute.machineTypes().list(projectId, zone).execute().getItems();
  }

  List<DiskType> listDiskTypes(String projectId, String zone) throws IOException {
    return compute.diskTypes().list(projectId, zone).execute().getItems();
  }

  Image getImage(String projectId, String image) throws IOException {
    return compute.images().get(projectId, image).execute();
  }

  List<Image> listImages(String projectId) throws IOException {
    return compute.images().list(projectId).execute().getItems();
  }

  List<AcceleratorType> listAcceleratorTypes(String projectId, String zone) throws IOException {
    return compute.acceleratorTypes().list(projectId, zone).execute().getItems();
  }

  List<Network> listNetworks(String projectId) throws IOException {
    return compute.networks().list(projectId).execute().getItems();
  }

  List<Subnetwork> listSubnetworks(String projectId, String region) throws IOException {
    return compute.subnetworks().list(projectId, region).execute().getItems();
  }

  Operation insertInstance(String projectId, String zone, Instance instance) throws IOException {
    return compute.instances().insert(projectId, zone, instance).execute();
  }

  Operation insertInstanceWithTemplate(
      String projectId, String zone, Instance instance, String template) throws IOException {
    return compute
        .instances()
        .insert(projectId, zone, instance)
        .setSourceInstanceTemplate(template)
        .execute();
  }

  Operation deleteInstance(String projectId, String zone, String instanceId) throws IOException {
    return compute.instances().delete(projectId, zone, instanceId).execute();
  }

  Operation setInstanceMetadata(String projectId, String zone, String instanceId, Metadata metadata)
      throws IOException {
    return compute.instances().setMetadata(projectId, zone, instanceId, metadata).execute();
  }

  Instance getInstance(String projectId, String zone, String instanceId) throws IOException {
    return compute.instances().get(projectId, zone, instanceId).execute();
  }

  Map<String, InstancesScopedList> aggregatedListInstances(String projectId, String filter)
      throws IOException {
    return compute.instances().aggregatedList(projectId).setFilter(filter).execute().getItems();
  }

  InstanceTemplate getInstanceTemplate(String projectId, String templateName) throws IOException {
    return compute.instanceTemplates().get(projectId, templateName).execute();
  }

  List<InstanceTemplate> listInstanceTemplates(String projectId) throws IOException {
    return compute.instanceTemplates().list(projectId).execute().getItems();
  }

  Operation insertInstanceTemplate(String projectId, InstanceTemplate instanceTemplate)
      throws IOException {
    return compute.instanceTemplates().insert(projectId, instanceTemplate).execute();
  }

  Operation deleteInstanceTemplate(String projectId, String templateName) throws IOException {
    return compute.instanceTemplates().delete(projectId, templateName).execute();
  }

  Operation createDiskSnapshot(String projectId, String zone, String disk, Snapshot snapshot)
      throws IOException {
    return compute.disks().createSnapshot(projectId, zone, disk, snapshot).execute();
  }

  Operation deleteSnapshot(String projectId, String snapshotName) throws IOException {
    return compute.snapshots().delete(projectId, snapshotName).execute();
  }

  Snapshot getSnapshot(String projectId, String snapshotName) throws IOException {
    return compute.snapshots().get(projectId, snapshotName).execute();
  }

  Operation getZoneOperation(String projectId, String zone, String operationId) throws IOException {
    return compute.zoneOperations().get(projectId, zone, operationId).execute();
  }

  Operation simulateMaintenanceEvent(
      final String projectId, final String zone, final String instanceId) throws IOException {
    return compute.instances().simulateMaintenanceEvent(projectId, zone, instanceId).execute();
  }
}
