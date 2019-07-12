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
import com.google.common.collect.ImmutableList;
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

  ComputeClient(final Compute compute) {
    this.compute = compute;
  }

  public static List<Metadata.Items> mergeMetadataItems(
      final List<Metadata.Items> winner, final List<Metadata.Items> loser) {
    if (loser == null) {
      return ImmutableList.copyOf(winner);
    }

    List<Metadata.Items> result = new ArrayList<>(winner);
    // Only add items from the existing list to the result if there are no duplicates by key
    loser
        .stream()
        .filter(
            existing ->
                winner.stream().noneMatch(newItem -> newItem.getKey().equals(existing.getKey())))
        .forEach(result::add);
    return ImmutableList.copyOf(result);
  }

  /**
   * @return
   * @throws IOException
   */
  public List<Region> getRegions(final String projectId) throws IOException {
    return processResourceList(
        compute.regions().list(projectId).execute().getItems(),
        r -> !isDeprecated(r.getDeprecated()),
        Comparator.comparing(Region::getName));
  }

  public List<Zone> getZones(final String projectId, final String region) throws IOException {
    return processResourceList(
        compute.zones().list(projectId).execute().getItems(),
        z -> region.equalsIgnoreCase(z.getRegion()),
        Comparator.comparing(Zone::getName));
  }

  public List<MachineType> getMachineTypes(final String projectId, final String zone)
      throws IOException {
    return processResourceList(
        compute.machineTypes().list(projectId, nameFromSelfLink(zone)).execute().getItems(),
        o -> !isDeprecated(o.getDeprecated()),
        Comparator.comparing(MachineType::getName));
  }

  public List<String> cpuPlatforms(final String projectId, final String zone) throws IOException {
    Zone zoneObject = compute.zones().get(projectId, zone).execute();
    if (zoneObject == null) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(zoneObject.getAvailableCpuPlatforms());
  }

  public List<DiskType> getDiskTypes(final String projectId, final String zone) throws IOException {
    return processResourceList(
        getDiskTypeList(projectId, zone),
        d -> !isDeprecated(d.getDeprecated()),
        Comparator.comparing(DiskType::getName));
  }

  public List<DiskType> getBootDiskTypes(final String projectId, final String zone)
      throws IOException {
    return processResourceList(
        this.getDiskTypeList(projectId, zone),
        // No local disks
        d -> !isDeprecated(d.getDeprecated()) && !d.getName().startsWith("local-"),
        Comparator.comparing(DiskType::getName));
  }

  private List<DiskType> getDiskTypeList(final String projectId, final String zone)
      throws IOException {
    return compute.diskTypes().list(projectId, nameFromSelfLink(zone)).execute().getItems();
  }

  public List<Image> getImages(final String projectId) throws IOException {
    return processResourceList(
        compute.images().list(projectId).execute().getItems(),
        i -> !isDeprecated(i.getDeprecated()),
        Comparator.comparing(Image::getName));
  }

  public Image getImage(final String projectId, final String name) throws IOException {
    return compute.images().get(projectId, name).execute();
  }

  public List<AcceleratorType> getAcceleratorTypes(final String projectId, final String zone)
      throws IOException {
    return processResourceList(
        compute.acceleratorTypes().list(projectId, nameFromSelfLink(zone)).execute().getItems(),
        a -> !isDeprecated(a.getDeprecated()),
        Comparator.comparing(AcceleratorType::getName));
  }

  public List<Network> getNetworks(final String projectId) throws IOException {
    return processResourceList(
        compute.networks().list(projectId).execute().getItems(),
        Comparator.comparing(Network::getName));
  }

  public List<Subnetwork> getSubnetworks(
      final String projectId, final String networkSelfLink, final String region)
      throws IOException {
    return processResourceList(
        compute.subnetworks().list(projectId, nameFromSelfLink(region)).execute().getItems(),
        s -> s.getNetwork().equalsIgnoreCase(networkSelfLink),
        Comparator.comparing(Subnetwork::getName));
  }

  public Operation insertInstance(
      final String projectId, final String template, final Instance instance) throws IOException {
    final Compute.Instances.Insert insert =
        compute.instances().insert(projectId, instance.getZone(), instance);
    if (!Strings.isNullOrEmpty(template)) {
      insert.setSourceInstanceTemplate(template);
    }
    return insert.execute();
  }

  public Operation terminateInstance(
      final String projectId, final String zone, final String InstanceId) throws IOException {
    return compute.instances().delete(projectId, nameFromSelfLink(zone), InstanceId).execute();
  }

  public Operation terminateInstanceWithStatus(
      final String projectId,
      final String zone,
      final String instanceId,
      final String desiredStatus)
      throws IOException, InterruptedException {
    final String zoneName = nameFromSelfLink(zone);
    Instance i = getInstance(projectId, zoneName, instanceId);
    if (i.getStatus().equals(desiredStatus)) {
      return compute.instances().delete(projectId, zoneName, instanceId).execute();
    }
    return null;
  }

  public Instance getInstance(final String projectId, final String zone, final String instanceId)
      throws IOException {
    return compute.instances().get(projectId, nameFromSelfLink(zone), instanceId).execute();
  }

  /**
   * Return all instances that contain the given labels
   *
   * @param projectId
   * @param labels
   * @return
   * @throws IOException
   */
  public List<Instance> getInstancesWithLabel(
      final String projectId, final Map<String, String> labels) throws IOException {
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

  public InstanceTemplate getTemplate(final String projectId, final String templateName)
      throws IOException {
    return compute.instanceTemplates().get(projectId, templateName).execute();
  }

  public void insertTemplate(final String projectId, InstanceTemplate instanceTemplate)
      throws IOException {
    compute.instanceTemplates().insert(projectId, instanceTemplate).execute();
  }

  public void deleteTemplate(final String projectId, final String templateName) throws IOException {
    compute.instanceTemplates().delete(projectId, templateName).execute();
  }

  public List<InstanceTemplate> getTemplates(final String projectId) throws IOException {
    return processResourceList(
        compute.instanceTemplates().list(projectId).execute().getItems(),
        Comparator.comparing(InstanceTemplate::getName));
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
  public void createSnapshot(final String projectId, final String zone, final String instanceId)
      throws IOException, InterruptedException {
    try {
      String zoneName = nameFromSelfLink(zone);
      Instance instance = compute.instances().get(projectId, zoneName, instanceId).execute();

      // TODO: JENKINS-56113 parallelize snapshot creation
      for (AttachedDisk disk : instance.getDisks()) {
        String diskId = nameFromSelfLink(disk.getSource());
        createSnapshotForDisk(projectId, zoneName, diskId);
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
  public void createSnapshotForDisk(final String projectId, final String zone, final String diskId)
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
  public void deleteSnapshot(final String projectId, final String snapshotName) throws IOException {
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
  public Snapshot getSnapshot(final String projectId, final String snapshotName)
      throws IOException {
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
      final String projectId,
      final String zone,
      final String instanceId,
      final List<Metadata.Items> items)
      throws IOException, InterruptedException {
    String zoneName = nameFromSelfLink(zone);
    Instance instance = getInstance(projectId, zoneName, instanceId);
    Metadata existingMetadata = instance.getMetadata();

    List<Metadata.Items> newMetadataItems = mergeMetadataItems(items, existingMetadata.getItems());
    existingMetadata.setItems(newMetadataItems);

    Operation op =
        compute
            .instances()
            .setMetadata(projectId, zoneName, instanceId, existingMetadata)
            .execute();
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
      final String projectId, final String operationId, final String zone, final long timeout)
      throws IOException, InterruptedException {
    if (Strings.isNullOrEmpty(operationId)) {
      throw new IllegalArgumentException("Operation ID can not be null");
    }
    if (Strings.isNullOrEmpty(zone)) {
      throw new IllegalArgumentException("Zone can not be null");
    }
    String zoneName = nameFromSelfLink(zone);

    Operation operation = compute.zoneOperations().get(projectId, zoneName, operationId).execute();
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
      Compute.ZoneOperations.Get get =
          compute.zoneOperations().get(projectId, zoneName, operationId);
      operation = get.execute();
      if (operation != null) {
        status = operation.getStatus();
      }
    }
    return operation.getError();
  }

  private static boolean isDeprecated(final DeprecationStatus deprecated) {
    return deprecated != null && deprecated.getState().equalsIgnoreCase("DEPRECATED");
  }
}
