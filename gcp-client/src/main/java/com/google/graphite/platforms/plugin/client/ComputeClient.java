/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.graphite.platforms.plugin.client;

import static com.google.graphite.platforms.plugin.client.util.ClientUtil.buildLabelsFilterString;
import static com.google.graphite.platforms.plugin.client.util.ClientUtil.nameFromSelfLink;
import static com.google.graphite.platforms.plugin.client.util.ClientUtil.processResourceList;

import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.AcceleratorType;
import com.google.api.services.compute.model.AttachedDisk;
import com.google.api.services.compute.model.DeprecationStatus;
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
import com.google.api.services.compute.model.SnapshotList;
import com.google.api.services.compute.model.Subnetwork;
import com.google.api.services.compute.model.Zone;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for communicating with the Google Compute API
 *
 * @see <a href="https://cloud.google.com/compute/">Cloud Engine</a>
 */
public class ComputeClient {
  private static final Logger LOGGER = Logger.getLogger(ComputeClient.class.getName());
  private static final long SNAPSHOT_TIMEOUT_MILLISECONDS = TimeUnit.MINUTES.toMillis(10);

  private final Compute compute;

  public ComputeClient(Compute compute) {
    this.compute = compute;
  }

  public static List<Metadata.Items> mergeMetadataItems(
      List<Metadata.Items> winner, List<Metadata.Items> loser) {
    if (loser == null) {
      loser = new ArrayList<>();
    }

    // Remove any existing metadata that has the same key(s) as what we're trying to update/append
    for (Metadata.Items existing : loser) {
      boolean duplicate = false;
      for (Metadata.Items newItem : winner) { // Items to append
        if (existing.getKey().equals(newItem.getKey())) {
          duplicate = true;
        }
      }
      if (!duplicate) {
        winner.add(existing);
      }
    }
    return winner;
  }

  /**
   * @return
   * @throws IOException
   */
  public List<Region> getRegions(String projectId) throws IOException {
    List<Region> regions = compute.regions().list(projectId).execute().getItems();
    return processResourceList(
        regions, r -> !isDeprecated(r.getDeprecated()), Comparator.comparing(Region::getName));
  }

  public List<Zone> getZones(String projectId, String region) throws IOException {
    List<Zone> zones = compute.zones().list(projectId).execute().getItems();
    return processResourceList(
        zones, z -> region.equalsIgnoreCase(z.getRegion()), Comparator.comparing(Zone::getName));
  }

  public List<MachineType> getMachineTypes(String projectId, String zone) throws IOException {
    zone = nameFromSelfLink(zone);
    List<MachineType> machineTypes =
        compute.machineTypes().list(projectId, zone).execute().getItems();
    return processResourceList(
        machineTypes,
        o -> !isDeprecated(o.getDeprecated()),
        Comparator.comparing(MachineType::getName));
  }

  public List<String> cpuPlatforms(String projectId, String zone) throws IOException {
    List<String> cpuPlatforms = new ArrayList<String>();
    zone = nameFromSelfLink(zone);
    Zone zoneObject = compute.zones().get(projectId, zone).execute();
    if (zoneObject == null) {
      return cpuPlatforms;
    }
    return zoneObject.getAvailableCpuPlatforms();
  }

  public List<DiskType> getDiskTypes(String projectId, String zone) throws IOException {
    zone = nameFromSelfLink(zone);
    List<DiskType> diskTypes = getDiskTypeList(projectId, zone);
    return processResourceList(
        diskTypes, d -> !isDeprecated(d.getDeprecated()), Comparator.comparing(DiskType::getName));
  }

  public List<DiskType> getBootDiskTypes(String projectId, String zone) throws IOException {
    zone = nameFromSelfLink(zone);
    List<DiskType> diskTypes = this.getDiskTypes(projectId, zone);

    // No local disks
    return processResourceList(
        diskTypes,
        d -> !isDeprecated(d.getDeprecated()) && !d.getName().startsWith("local-"),
        Comparator.comparing(DiskType::getName));
  }

  private List<DiskType> getDiskTypeList(String projectId, String zone) throws IOException {
    return compute.diskTypes().list(projectId, zone).execute().getItems();
  }

  public List<Image> getImages(String projectId) throws IOException {
    List<Image> images = compute.images().list(projectId).execute().getItems();
    return processResourceList(
        images, i -> !isDeprecated(i.getDeprecated()), Comparator.comparing(Image::getName));
  }

  public Image getImage(String projectId, String name) throws IOException {
    Image image = compute.images().get(projectId, name).execute();

    return image;
  }

  public List<AcceleratorType> getAcceleratorTypes(String projectId, String zone)
      throws IOException {
    zone = nameFromSelfLink(zone);

    List<AcceleratorType> acceleratorTypes =
        compute.acceleratorTypes().list(projectId, zone).execute().getItems();
    return processResourceList(
        acceleratorTypes,
        a -> !isDeprecated(a.getDeprecated()),
        Comparator.comparing(AcceleratorType::getName));
  }

  public List<Network> getNetworks(String projectId) throws IOException {
    List<Network> networks = compute.networks().list(projectId).execute().getItems();
    return processResourceList(networks, Comparator.comparing(Network::getName));
  }

  public List<Subnetwork> getSubnetworks(String projectId, String networkSelfLink, String region)
      throws IOException {
    region = nameFromSelfLink(region);
    List<Subnetwork> subnetworks =
        compute.subnetworks().list(projectId, region).execute().getItems();
    return processResourceList(
        subnetworks,
        s -> s.getNetwork().equalsIgnoreCase(networkSelfLink),
        Comparator.comparing(Subnetwork::getName));
  }

  public Operation insertInstance(String projectId, String template, Instance instance)
      throws IOException {
    final Compute.Instances.Insert insert =
        compute.instances().insert(projectId, instance.getZone(), instance);
    if (!Strings.isNullOrEmpty(template)) {
      insert.setSourceInstanceTemplate(template);
    }
    return insert.execute();
  }

  public Operation terminateInstance(String projectId, String zone, String InstanceId)
      throws IOException {
    zone = nameFromSelfLink(zone);
    return compute.instances().delete(projectId, zone, InstanceId).execute();
  }

  public Operation terminateInstanceWithStatus(
      String projectId, String zone, String instanceId, String desiredStatus)
      throws IOException, InterruptedException {
    zone = nameFromSelfLink(zone);
    Instance i = getInstance(projectId, zone, instanceId);
    if (i.getStatus().equals(desiredStatus)) {
      return compute.instances().delete(projectId, zone, instanceId).execute();
    }
    return null;
  }

  public Instance getInstance(String projectId, String zone, String instanceId) throws IOException {
    zone = nameFromSelfLink(zone);
    return compute.instances().get(projectId, zone, instanceId).execute();
  }

  /**
   * Return all instances that contain the given labels
   *
   * @param projectId
   * @param labels
   * @return
   * @throws IOException
   */
  public List<Instance> getInstancesWithLabel(String projectId, Map<String, String> labels)
      throws IOException {
    Compute.Instances.AggregatedList request = compute.instances().aggregatedList(projectId);
    request.setFilter(buildLabelsFilterString(labels));
    Map<String, InstancesScopedList> result = request.execute().getItems();
    List<Instance> instances = new ArrayList<>();
    for (InstancesScopedList instancesInZone : result.values()) {
      if (instancesInZone.getInstances() != null) {
        instances.addAll(instancesInZone.getInstances());
      }
    }
    return instances;
  }

  public InstanceTemplate getTemplate(String projectId, String templateName) throws IOException {
    return compute.instanceTemplates().get(projectId, templateName).execute();
  }

  public void insertTemplate(String projectId, InstanceTemplate instanceTemplate)
      throws IOException {
    compute.instanceTemplates().insert(projectId, instanceTemplate).execute();
  }

  public void deleteTemplate(String projectId, String templateName) throws IOException {
    compute.instanceTemplates().delete(projectId, templateName).execute();
  }

  public List<InstanceTemplate> getTemplates(String projectId) throws IOException {
    List<InstanceTemplate> instanceTemplates =
        compute.instanceTemplates().list(projectId).execute().getItems();
    return processResourceList(instanceTemplates, Comparator.comparing(InstanceTemplate::getName));
  }

  /**
   * Creates persistent disk snapshot for Compute Engine instance. This method blocks until the
   * operation completes.
   *
   * @param projectId Google cloud project id (e.g. my-project).
   * @param zone Instance's zone.
   * @param instanceId Name of the instance whose disks to take a snapshot of.
   * @throws IOException If an error occured in snapshot creation.
   * @throws InterruptedException If snapshot creation is interrupted.
   */
  public void createSnapshot(String projectId, String zone, String instanceId)
      throws IOException, InterruptedException {
    try {
      zone = nameFromSelfLink(zone);
      Instance instance = compute.instances().get(projectId, zone, instanceId).execute();

      // TODO: JENKINS-56113 parallelize snapshot creation
      for (AttachedDisk disk : instance.getDisks()) {
        String diskId = nameFromSelfLink(disk.getSource());
        createSnapshotForDisk(projectId, zone, diskId);
      }
    } catch (InterruptedException ie) {
      // catching InterruptedException here because calling function also can throw
      // InterruptedException from trying to terminate node
      LOGGER.log(Level.WARNING, "Error in creating snapshot.", ie);
      throw ie;
    } catch (IOException ioe) {
      LOGGER.log(Level.WARNING, "Interruption in creating snapshot.", ioe);
      throw ioe;
    }
  }

  /**
   * Given a disk's name, create a snapshot for said disk.
   *
   * @param projectId Google cloud project id.
   * @param zone Zone of disk.
   * @param diskId Name of disk to create a snapshot for.
   * @throws IOException If an error occured in snapshot creation.
   * @throws InterruptedException If snapshot creation is interrupted.
   */
  public void createSnapshotForDisk(String projectId, String zone, String diskId)
      throws IOException, InterruptedException {
    Snapshot snapshot = new Snapshot();
    snapshot.setName(diskId);

    Operation op = compute.disks().createSnapshot(projectId, zone, diskId, snapshot).execute();
    // poll for result
    waitForOperationCompletion(
        projectId, op.getName(), op.getZone(), SNAPSHOT_TIMEOUT_MILLISECONDS);
  }

  /**
   * Deletes persistent disk snapshot. Does not block.
   *
   * @param projectId Google cloud project id.
   * @param snapshotName Name of the snapshot to be deleted.
   * @throws IOException If an error occurred in deleting the snapshot.
   */
  public void deleteSnapshot(String projectId, String snapshotName) throws IOException {
    compute.snapshots().delete(projectId, snapshotName).execute();
  }

  /**
   * Returns snapshot with name snapshotName
   *
   * @param projectId Google cloud project id.
   * @param snapshotName Name of the snapshot to get.
   * @return Snapshot object with given snapshotName. Null if not found.
   * @throws IOException If an error occurred in retrieving the snapshot.
   */
  public Snapshot getSnapshot(String projectId, String snapshotName) throws IOException {
    SnapshotList response;
    Compute.Snapshots.List request = compute.snapshots().list(projectId);

    do {
      response = request.execute();
      if (response.getItems() == null) {
        continue;
      }
      for (Snapshot snapshot : response.getItems()) {
        if (snapshotName.equals(snapshot.getName())) return snapshot;
      }
      request.setPageToken(response.getNextPageToken());
    } while (response.getNextPageToken() != null);

    return null;
  }

  /**
   * Appends metadata to an instance. Any metadata items with existing keys will be overwritten.
   * Otherwise, metadata is preserved. This method blocks until the operation completes.
   *
   * @param projectId
   * @param zone
   * @param instanceId
   * @param items
   * @throws IOException
   * @throws InterruptedException
   */
  public Operation.Error appendInstanceMetadata(
      String projectId, String zone, String instanceId, List<Metadata.Items> items)
      throws IOException, InterruptedException {
    zone = nameFromSelfLink(zone);
    Instance instance = getInstance(projectId, zone, instanceId);
    Metadata existingMetadata = instance.getMetadata();

    List<Metadata.Items> newMetadataItems = mergeMetadataItems(items, existingMetadata.getItems());
    existingMetadata.setItems(newMetadataItems);

    Operation op =
        compute.instances().setMetadata(projectId, zone, instanceId, existingMetadata).execute();
    return waitForOperationCompletion(projectId, op.getName(), op.getZone(), 60 * 1000);
  }

  /**
   * Blocks until an existing operation completes.
   *
   * @param projectId
   * @param operationId
   * @param zone
   * @param timeout
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public Operation.Error waitForOperationCompletion(
      String projectId, String operationId, String zone, long timeout)
      throws IOException, InterruptedException {
    if (Strings.isNullOrEmpty(operationId)) {
      throw new IllegalArgumentException("Operation ID can not be null");
    }
    if (Strings.isNullOrEmpty(zone)) {
      throw new IllegalArgumentException("Zone can not be null");
    }
    if (zone != null) {
      String[] bits = zone.split("/");
      zone = bits[bits.length - 1];
    }

    Operation operation = compute.zoneOperations().get(projectId, zone, operationId).execute();
    long start = System.currentTimeMillis();
    final long POLL_INTERVAL = 5 * 1000;

    String status = operation.getStatus();
    while (!status.equals("DONE")) {
      Thread.sleep(POLL_INTERVAL);
      long elapsed = System.currentTimeMillis() - start;
      if (elapsed >= timeout) {
        throw new InterruptedException("Timed out waiting for operation to complete");
      }
      LOGGER.log(Level.FINE, "Waiting for operation " + operationId + " to complete..");
      if (zone != null) {
        Compute.ZoneOperations.Get get = compute.zoneOperations().get(projectId, zone, operationId);
        operation = get.execute();
      } else {
        Compute.GlobalOperations.Get get = compute.globalOperations().get(projectId, operationId);
        operation = get.execute();
      }
      if (operation != null) {
        status = operation.getStatus();
      }
    }
    return operation.getError();
  }

  private static boolean isDeprecated(DeprecationStatus deprecated) {
    return deprecated != null && deprecated.getState().equalsIgnoreCase("DEPRECATED");
  }
}
