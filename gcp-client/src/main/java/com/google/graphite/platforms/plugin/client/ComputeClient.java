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

import com.diffplug.common.base.Errors;
import com.google.api.services.compute.Compute.Instances.SimulateMaintenanceEvent;
import com.google.api.services.compute.model.AcceleratorType;
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
import com.google.api.services.compute.model.Subnetwork;
import com.google.api.services.compute.model.Zone;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

/**
 * Client for communicating with the Google Compute API.
 *
 * @see <a href="https://cloud.google.com/compute/">Compute Engine</a>
 */
public class ComputeClient {
  private static final Logger LOGGER = Logger.getLogger(ComputeClient.class.getName());
  private static final long POLLING_INTERVAL = 5 * 1000;

  private final ComputeWrapper compute;

  ComputeClient(final ComputeWrapper compute) {
    this.compute = compute;
  }

  /**
   * Helper method for merging lists of {@link Metadata.Items}.
   *
   * @param winner The list of items that will be returned in the final result.
   * @param loser The list of items that will be returned in the final result, unless an item has
   *     the same key as an item in {@param winner}, in which case the item from winner will be
   *     used.
   * @return The combined list of items from the input lists.
   */
  public static ImmutableList<Metadata.Items> mergeMetadataItems(
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
   * Retrieves the list of {@link Region}s available to the provided project.
   *
   * @param projectId The ID of the project to check.
   * @return A sorted list of available {@link Region}s. Deprecated items are excluded.
   * @throws IOException An error occurred attempting to get the list of regions.
   */
  public ImmutableList<Region> listRegions(final String projectId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    return processResourceList(
        compute.listRegions(projectId),
        r -> !isDeprecated(r.getDeprecated()),
        Comparator.comparing(Region::getName));
  }

  /**
   * Retrieves the list of zones available to the project in the given region.
   *
   * @param projectId The ID of the project to check.
   * @param regionLink The self link of the region to check.
   * @return A list of available {@link Zone}s sorted by name.
   * @throws IOException An error occurred attempting to get the list of zones.
   */
  public ImmutableList<Zone> listZones(final String projectId, final String regionLink)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(regionLink));
    return processResourceList(
        compute.listZones(projectId),
        z -> regionLink.equalsIgnoreCase(z.getRegion()),
        Comparator.comparing(Zone::getName));
  }

  /**
   * Retrieves the list of {@link MachineType}s available for the given project and zone.
   *
   * @param projectId The ID of the project to check.
   * @param zoneLink The self link of the zone to check.
   * @return A list of available {@link MachineType}s sorted by name. Deprecated items are excluded.
   * @throws IOException An error occurred attempting to get the list of machine types.
   */
  public ImmutableList<MachineType> listMachineTypes(final String projectId, final String zoneLink)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    return processResourceList(
        compute.listMachineTypes(projectId, nameFromSelfLink(zoneLink)),
        o -> !isDeprecated(o.getDeprecated()),
        Comparator.comparing(MachineType::getName));
  }

  /**
   * Retrieves the list of CPU Platforms available for the given project and zone.
   *
   * @param projectId The ID of the project to check.
   * @param zoneLink The self link of the zone to check.
   * @return A sorted list of strings with the available CPU platforms.
   * @throws IOException An error occurred attempting to get the list of CPU platforms.
   */
  public ImmutableList<String> listCpuPlatforms(final String projectId, final String zoneLink)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    return processResourceList(
        compute.getZone(projectId, nameFromSelfLink(zoneLink)).getAvailableCpuPlatforms(),
        String::compareTo);
  }

  /**
   * Retrieves the list of {@link DiskType}s available for the given project and zone.
   *
   * @param projectId The ID of the project to check.
   * @param zoneLink The self link of the zone to check.
   * @return A sorted list of available {@link DiskType}s. Deprecated disks are excluded.
   * @throws IOException An error occurred attempting to get the list of disk types.
   */
  public ImmutableList<DiskType> listDiskTypes(final String projectId, final String zoneLink)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    return processResourceList(
        compute.listDiskTypes(projectId, nameFromSelfLink(zoneLink)),
        d -> !isDeprecated(d.getDeprecated()),
        Comparator.comparing(DiskType::getName));
  }

  /**
   * Retrieves the list of Boot {@link DiskType}s available for the given project and zone.
   *
   * @param projectId The ID of the project to check.
   * @param zoneLink The self link of the zone to check.
   * @return A list of available {@link DiskType}s sorted by name. Deprecated and local disks are
   *     excluded.
   * @throws IOException An error occurred attempting to get the list of disk types.
   */
  public ImmutableList<DiskType> listBootDiskTypes(final String projectId, final String zoneLink)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    return processResourceList(
        compute.listDiskTypes(projectId, nameFromSelfLink(zoneLink)),
        // No local disks
        d -> !isDeprecated(d.getDeprecated()) && !d.getName().startsWith("local-"),
        Comparator.comparing(DiskType::getName));
  }

  /**
   * Retrieves the list of {@link Image}s available for the given project.
   *
   * @param projectId The ID of the project to check.
   * @return A list of available {@link Image}s. Deprecated items are excluded.
   * @throws IOException An error occurred attempting to get the list of images.
   */
  public ImmutableList<Image> listImages(final String projectId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    return processResourceList(
        compute.listImages(projectId),
        i -> !isDeprecated(i.getDeprecated()),
        Comparator.comparing(Image::getName));
  }

  /**
   * Retrieves the {@link Image} with the given name in the given project.
   *
   * @param projectId The ID of the project to check.
   * @param imageName The name of the image to retrieve.
   * @return The {@link Image} to retrieve.
   * @throws IOException An error occurred attempting to get the image.
   */
  public Image getImage(final String projectId, final String imageName) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(imageName));
    return compute.getImage(projectId, imageName);
  }

  /**
   * Retrieves the list of {@link AcceleratorType}s available for the given project and zone.
   *
   * @param projectId The ID of the project to check.
   * @param zoneLink The self link of the zone to check.
   * @return A list of available {@link AcceleratorType}s sorted by name. Deprecated items are
   *     excluded.
   * @throws IOException An error occurred attempting to get the list of accelerator types.
   */
  public ImmutableList<AcceleratorType> listAcceleratorTypes(
      final String projectId, final String zoneLink) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    return processResourceList(
        compute.listAcceleratorTypes(projectId, nameFromSelfLink(zoneLink)),
        a -> !isDeprecated(a.getDeprecated()),
        Comparator.comparing(AcceleratorType::getName));
  }

  /**
   * Retrieves the list of {@link Network}s available for the given project.
   *
   * @param projectId The ID of the project to check.
   * @return A list of available {@link Network}s sorted by name.
   * @throws IOException An error occurred attempting to get the list of networks.
   */
  public ImmutableList<Network> listNetworks(final String projectId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    return processResourceList(
        compute.listNetworks(projectId), Comparator.comparing(Network::getName));
  }

  /**
   * Retrieves the list of {@link Subnetwork}s of the network in the given project and region.
   *
   * @param projectId The ID of the project to check.
   * @param networkLink The self link of the network to check.
   * @param regionLink The self link of the region to check.
   * @return A list of available {@link Subnetwork}s sorted by name.
   * @throws IOException An error occurred attempting to get the list of machine types.
   */
  public ImmutableList<Subnetwork> listSubnetworks(
      final String projectId, final String networkLink, final String regionLink)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(networkLink));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(regionLink));
    return processResourceList(
        compute.listSubnetworks(projectId, nameFromSelfLink(regionLink)),
        s -> s.getNetwork().equalsIgnoreCase(networkLink),
        Comparator.comparing(Subnetwork::getName));
  }

  /**
   * Inserts the provided instance into the given project. This method does not block on the
   * completion of the operation.
   *
   * @param projectId The ID of the project where the instance will reside.
   * @param templateLink A self link to an instance template that may be optionally specified to use
   *     the configuration for that template when creating the instance.
   * @param instance An {@link Instance} to insert. The instance should specify a zone at minimum.
   * @return The insert {@link Operation} for tracking the status of inserting the instance.
   * @throws IOException There was an error attempting to insert the instance.
   */
  public Operation insertInstance(
      final String projectId, final Optional<String> templateLink, final Instance instance)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkNotNull(instance);
    String zone = nameFromSelfLink(instance.getZone());
    if (templateLink.isPresent() && !templateLink.get().isEmpty()) {
      return compute.insertInstanceWithTemplate(projectId, zone, instance, templateLink.get());
    }
    return compute.insertInstance(projectId, zone, instance);
  }

  /**
   * Deletes the {@link Instance} specified with the given ID in the project and zone. This method
   * does not block on the completion of the operation.
   *
   * @param projectId The ID of the project where the instance resides.
   * @param zoneLink The self link of the zone where the instance resides.
   * @param instanceId The ID of the instance to delete.
   * @return The deletion {@link Operation} for tracking the status of deleting the instance.
   * @throws IOException There was an error attempting to delete the instance.
   */
  public Operation terminateInstanceAsync(
      final String projectId, final String zoneLink, final String instanceId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId));
    return compute.deleteInstance(projectId, nameFromSelfLink(zoneLink), instanceId);
  }

  /**
   * Deletes the {@link Instance} specified with the given ID in the project and zone if it has the
   * provided status. This method does not block on the completion of the operation.
   *
   * @param projectId The ID of the project where the instance resides.
   * @param zoneLink The self link of the zone where the instance resides.
   * @param instanceId The ID of the instance to delete.
   * @param desiredStatus The status that the instance must have to be deleted.
   * @return The deletion {@link Operation} for tracking the status of deleting the instance, or
   *     empty if the instance did not have the desired status.
   * @throws IOException There was an error attempting to get the instance status or delete the
   *     instance.
   */
  public Optional<Operation> terminateInstanceWithStatusAsync(
      final String projectId,
      final String zoneLink,
      final String instanceId,
      final String desiredStatus)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(desiredStatus));
    final String zoneName = nameFromSelfLink(zoneLink);
    Instance instance = compute.getInstance(projectId, zoneName, instanceId);
    if (instance.getStatus().equals(desiredStatus)) {
      return Optional.of(compute.deleteInstance(projectId, zoneName, instanceId));
    }
    return Optional.empty();
  }

  /**
   * Retrieves the {@link Instance} with the given ID in the given project and zone.
   *
   * @param projectId The ID of the project to check.
   * @param zoneLink The self link of the zone to check.
   * @param instanceId The ID of the instance to retrieve.
   * @return The specified {@link Instance}.
   * @throws IOException An error occurred attempting to get the instance.
   */
  public Instance getInstance(
      final String projectId, final String zoneLink, final String instanceId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId));
    return compute.getInstance(projectId, nameFromSelfLink(zoneLink), instanceId);
  }

  /**
   * Retrieves the list of {@link Instance}s with the provided labels in the project.
   *
   * @param projectId The ID of the project to check.
   * @param labels A map of labels.
   * @return A list of {@link Instance}s.
   * @throws IOException An error occurred attempting to get the list of machine types.
   */
  public ImmutableList<Instance> listInstancesWithLabel(
      final String projectId, final Map<String, String> labels) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkNotNull(labels);
    Map<String, InstancesScopedList> result =
        compute.aggregatedListInstances(projectId, buildLabelsFilterString(labels));
    List<Instance> instances = new ArrayList<>();
    for (InstancesScopedList matchingInstances : result.values()) {
      if (matchingInstances.getInstances() != null) {
        instances.addAll(matchingInstances.getInstances());
      }
    }
    return ImmutableList.copyOf(instances);
  }

  /**
   * Retrieves the {@link InstanceTemplate} with the given name in the project.
   *
   * @param projectId The ID of the project to check.
   * @param templateName The name of the template to retrieve.
   * @return The specified {@link InstanceTemplate}.
   * @throws IOException An error occurred attempting to get the instance template.
   */
  public InstanceTemplate getTemplate(final String projectId, final String templateName)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(templateName));
    return compute.getInstanceTemplate(projectId, templateName);
  }

  /**
   * Inserts the provided instance into the given project. This method does not block on the
   * completion of the operation.
   *
   * @param projectId The ID of the project where the instance template will reside.
   * @param instanceTemplate An {@link InstanceTemplate} to insert.
   * @return The insert {@link Operation} for tracking the status of inserting the instance
   *     template.
   * @throws IOException There was an error attempting to insert the instance template.
   */
  public Operation insertTemplateAsync(final String projectId, InstanceTemplate instanceTemplate)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkNotNull(instanceTemplate);
    return compute.insertInstanceTemplate(projectId, instanceTemplate);
  }

  /**
   * Deletes the {@link InstanceTemplate} with the given name in the given project. This method does
   * not block on the completion of the operation.
   *
   * @param projectId The ID of the project where the instance template resides.
   * @param templateName The name of the instance template to delete.
   * @return The deletion {@link Operation} for tracking the status of deleting the instance
   *     template.
   * @throws IOException There was an error attempting to delete the instance template.
   */
  public Operation deleteTemplateAsync(final String projectId, final String templateName)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(templateName));
    return compute.deleteInstanceTemplate(projectId, templateName);
  }

  /**
   * Retrieves the list of {@link InstanceTemplate}s available for the given project.
   *
   * @param projectId The ID of the project to check.
   * @return A list of available {@link InstanceTemplate}s sorted by name.
   * @throws IOException An error occurred attempting to get the list of instance templates.
   */
  public ImmutableList<InstanceTemplate> listTemplates(final String projectId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    return processResourceList(
        compute.listInstanceTemplates(projectId), Comparator.comparing(InstanceTemplate::getName));
  }

  /**
   * Creates persistent disk snapshot for Compute Engine instance. This method blocks until the
   * operation completes.
   *
   * @param projectId The ID of the project where instance resides.
   * @param zoneLink Self link of the instance's zone.
   * @param instanceId Name of the instance whose disks to take a snapshot of.
   * @param timeout The number of milliseconds to wait for snapshot creation.
   * @throws IOException If an error occurred in starting the snapshot creation {@link Operation} or
   *     when retrieving the instance.
   * @throws InterruptedException If snapshot creation is interrupted.
   * @throws OperationException If any errors occurred during the operation.
   */
  public void createSnapshotSync(
      final String projectId, final String zoneLink, final String instanceId, final long timeout)
      throws IOException, InterruptedException, OperationException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId));
    Preconditions.checkArgument(timeout > 0);
    String zoneName = nameFromSelfLink(zoneLink);
    Instance instance;
    try {
      instance = compute.getInstance(projectId, zoneName, instanceId);
    } catch (IOException ioe) {
      LOGGER.log(Level.WARNING, "Error retrieving instance.", ioe);
      throw ioe;
    }

    instance
        .getDisks()
        .parallelStream()
        .forEach(
            Errors.rethrow()
                .wrap(
                    disk -> {
                      try {
                        createSnapshotForDiskSync(
                            projectId, zoneName, nameFromSelfLink(disk.getSource()), timeout);
                      } catch (IOException ioe) {
                        LOGGER.log(Level.WARNING, "Error in creating snapshot.", ioe);
                        throw ioe;
                      } catch (InterruptedException ie) {
                        /* catching InterruptedException here because calling function also can
                         * throw InterruptedException from trying to terminate node */
                        LOGGER.log(Level.WARNING, "Interruption in creating snapshot.", ie);
                        throw ie;
                      } catch (OperationException oe) {
                        LOGGER.log(Level.WARNING, "Error in completing operation.");
                        throw oe;
                      }
                    }));
  }

  /**
   * Given a disk's name, create a snapshot for said disk. This method blocks until the operation
   * complete.
   *
   * @param projectId The ID of the project where the disk resides.
   * @param zoneName Zone of disk.
   * @param diskName Name of disk to create a snapshot for.
   * @param timeout The number of milliseconds to wait for snapshot creation.
   * @return The {@link Operation} that was performed.
   * @throws IOException If an error occurred in starting the snapshot creation operation.
   * @throws InterruptedException If snapshot creation is interrupted.
   * @throws OperationException If any errors occurred during the operation.
   */
  public Operation createSnapshotForDiskSync(
      final String projectId, final String zoneName, final String diskName, final long timeout)
      throws IOException, InterruptedException, OperationException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(diskName));
    Preconditions.checkArgument(timeout > 0);
    Snapshot snapshot = new Snapshot();
    snapshot.setName(diskName);

    Operation op = compute.createDiskSnapshot(projectId, zoneName, diskName, snapshot);
    return waitForOperationCompletion(projectId, op, timeout);
  }

  /**
   * Deletes persistent disk snapshot. Does not block.
   *
   * @param projectId The ID of the project where the snapshot resides.
   * @param snapshotName Name of the snapshot to be deleted.
   * @throws IOException If an error occurred in deleting the snapshot.
   */
  public Operation deleteSnapshotAsync(final String projectId, final String snapshotName)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(snapshotName));
    return compute.deleteSnapshot(projectId, snapshotName);
  }

  /**
   * Returns snapshot with name snapshotName.
   *
   * @param projectId The ID of the project where the project resides.
   * @param snapshotName Name of the snapshot to get.
   * @return Snapshot object with given snapshotName. Null if not found.
   * @throws IOException If an error occurred in retrieving the snapshot.
   */
  public Snapshot getSnapshot(final String projectId, final String snapshotName)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(snapshotName));
    return compute.getSnapshot(projectId, snapshotName);
  }

  /**
   * Return an operation in the provided zone.
   *
   * @param projectId The ID of the project for the {@link Operation} to retrieve.
   * @param zoneLink The self link of the zone for the {@link Operation} to retrieve.
   * @param operationId The ID of the {@link Operation} to retrieve.
   * @return The {@link Operation} specified by the inputs.
   * @throws IOException There was an error retrieving the specified {@link Operation}.
   */
  public Operation getZoneOperation(
      final String projectId, final String zoneLink, final String operationId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(operationId));
    return compute.getZoneOperation(projectId, nameFromSelfLink(zoneLink), operationId);
  }

  /**
   * Appends metadata to an instance. Any metadata items with existing keys will be overwritten.
   * Otherwise, metadata is preserved. This method blocks until the operation completes.
   *
   * @param projectId The ID of the project for the instance.
   * @param zoneLink The self link of the zone for the instance.
   * @param instanceId The ID of the instance.
   * @param items The new metadata items to append to existing metadata.
   * @param timeout The number of milliseconds to wait for the operation to timeout.
   * @return The final state of the {@link Operation} that was performed.
   * @throws IOException If there was an error retrieving the instance or starting the append
   *     operation.
   * @throws InterruptedException If the operation to set metadata timed out.
   * @throws OperationException If any errors occurred during the append operation.
   */
  public Operation appendInstanceMetadataSync(
      final String projectId,
      final String zoneLink,
      final String instanceId,
      final List<Metadata.Items> items,
      final long timeout)
      throws IOException, InterruptedException, OperationException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId));
    Preconditions.checkNotNull(items);
    Preconditions.checkArgument(timeout > 0);
    String zoneName = nameFromSelfLink(zoneLink);
    Instance instance = getInstance(projectId, zoneName, instanceId);
    Metadata existingMetadata = instance.getMetadata();

    List<Metadata.Items> newMetadataItems = mergeMetadataItems(items, existingMetadata.getItems());
    existingMetadata.setItems(newMetadataItems);

    Operation op = compute.setInstanceMetadata(projectId, zoneName, instanceId, existingMetadata);
    return waitForOperationCompletion(projectId, op, timeout);
  }

  /**
   * Blocks until an existing {@link Operation} completes.
   *
   * @param projectId The ID of the project for this {@link Operation}.
   * @param operation The {@link Operation} to reference.
   * @param timeout The number of milliseconds to wait for the {@link Operation} to complete.
   * @return The final state for the completed {@link Operation}.
   * @throws InterruptedException If the operation was not completed before the timeout.
   */
  public Operation waitForOperationCompletion(
      final String projectId, final Operation operation, final long timeout)
      throws InterruptedException, OperationException {
    // Intentionally omit other argument checks to use the ones in the other method.
    Preconditions.checkNotNull(operation);
    return waitForOperationCompletion(projectId, operation.getName(), operation.getZone(), timeout);
  }

  /**
   * Blocks until an existing {@link Operation} completes.
   *
   * @param projectId The ID of the project for this {@link Operation}.
   * @param operationName The name of the {@link Operation}.
   * @param zoneLink The self-link of the zone for the {@link Operation}.
   * @param timeout The number of milliseconds to wait for the {@link Operation} to complete.
   * @return The final state for the completed {@link Operation}.
   * @throws InterruptedException If the operation was not completed before the timeout.
   */
  public Operation waitForOperationCompletion(
      final String projectId, final String operationName, final String zoneLink, final long timeout)
      throws InterruptedException, OperationException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(operationName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    Preconditions.checkArgument(timeout > 0);

    // Used to hold the Operation.Error which comes from polling the Operation in lambda expression.
    Operation operation = new Operation();
    try {
      Awaitility.await()
          .pollInterval(POLLING_INTERVAL, TimeUnit.MILLISECONDS)
          .timeout(timeout, TimeUnit.MILLISECONDS)
          // Awaitility requires a function without arguments, so cannot use helper method here.
          .until(
              () -> {
                LOGGER.log(Level.FINE, "Waiting for operation " + operationName + " to complete.");
                try {
                  Operation op = getZoneOperation(projectId, zoneLink, operationName);
                  // Store the error here.
                  operation.setError(op.getError());
                  return isOperationDone(op);
                } catch (IOException ioe) {
                  LOGGER.log(Level.WARNING, "Error retrieving operation.", ioe);
                  return false;
                }
              });
    } catch (ConditionTimeoutException e) {
      throw new InterruptedException("Timed out waiting for operation to complete.");
    }
    if (operation.getError() != null) {
      throw new OperationException(operation.getError());
    }
    return operation;
  }

  /**
   * Simulate maintenance event on the {@link Instance}, with the given ID on the project requiring
   * either live migration or termination.
   *
   * @param projectId The ID of the project where the instance resides.
   * @param zoneLink The self link of the zone where the instance resides
   * @param instanceId The ID of the instance to simulate maintenance on.
   * @return The {@link SimulateMaintenanceEvent} triggered by this call.
   * @throws IOException If there was an error in referencing the instance or performing the
   *     simulated maintenance event
   */
  public SimulateMaintenanceEvent simulateMaintenanceEvent(
      final String projectId, final String zoneLink, final String instanceId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zoneLink));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId));
    return compute.simulateMaintenanceEvent(projectId, nameFromSelfLink(zoneLink), instanceId);
  }

  private boolean isOperationDone(final Operation operation) {
    if (operation == null) {
      return false;
    }
    return operation.getStatus().equals("DONE");
  }

  private static boolean isDeprecated(final DeprecationStatus deprecated) {
    return deprecated != null && deprecated.getState().equalsIgnoreCase("DEPRECATED");
  }

  /** Wraps Operation.Error so that the exceptions must be caught. */
  public static class OperationException extends Exception {
    private Operation.Error error;

    /**
     * Constructor for {@link OperationException}.
     *
     * @param error An {@link Operation.Error} generated from a GCP {@link Operation}.
     */
    public OperationException(Operation.Error error) {
      this("", error);
    }

    /**
     * Constructor for {@link OperationException}.
     *
     * @param message A specialized message to use for this exception.
     * @param error An {@link Operation.Error} generated from a GCP {@link Operation}.
     */
    public OperationException(String message, Operation.Error error) {
      super(message);
      this.error = error;
    }

    /**
     * Retrieves the error that this exception wraps so that more specific information can be
     * obtained when the exception is caught.
     *
     * @return The {@link Operation.Error} that this wraps.
     */
    public Operation.Error getError() {
      return error;
    }
  }
}
