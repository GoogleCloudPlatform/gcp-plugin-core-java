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

package com.google.cloud.graphite.platforms.plugin.client.util;

import com.google.cloud.graphite.platforms.plugin.client.model.InstanceResourceData;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** A library of utility functions for common interactions with the GCP client libraries. */
public class ClientUtil {
  private static final Pattern INSTANCE_RESOURCE_DATA_PATTERN =
      Pattern.compile(
          "https:\\/\\/www.googleapis.com\\/compute\\/v1\\/projects\\/([0-9a-zA-Z\\-]+)\\/zones\\/([a-z0-9\\-]+)\\/instances\\/([0-9a-zA-Z\\-]+)");

  /**
   * Given a list of GCP resources, filters the list using the provided filter and sorts according
   * to the provided comparator.
   *
   * @param items A list of GCP resources produced by a GCP client library request.
   * @param filter A predicate applied to objects in items. If true for a given object, then that
   *     object will be kept in the final result.
   * @param comparator Defines a comparison between any two objects in items used for sorting the
   *     result.
   * @param <T> The type of the list elements.
   * @return An {@link ImmutableList} which is empty if items is null or empty, and otherwise is
   *     items filtered and sorted as described above.
   */
  public static <T> ImmutableList<T> processResourceList(
      final List<T> items, final Predicate<T> filter, final Comparator<T> comparator) {
    if (items == null) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(
        items.stream().filter(filter).sorted(comparator).collect(Collectors.toList()));
  }

  /**
   * Given a list of GCP resources, sorts according to the provided comparator.
   *
   * @param items A list of GCP resources produced by a GCP client library request.
   * @param comparator Defines a comparison between any two objects in items used for sorting the
   *     result.
   * @param <T> The type of the list elements.
   * @return An {@link ImmutableList} which is empty if items is null or empty, and otherwise is
   *     items filtered and sorted as described above.
   */
  public static <T> ImmutableList<T> processResourceList(
      final List<T> items, final Comparator<T> comparator) {
    return processResourceList(items, n -> true, comparator);
  }

  /**
   * Removes the prefix information from the provided self link returning only the name.
   *
   * @param selfLink A GCP resource self link, e.g. for an instance:
   *     "projects/exampleproject/zones/us-west1-a/instances/example".
   * @return The name of the resource referenced by the self-link, i.e. "example".
   */
  public static String nameFromSelfLink(final String selfLink) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(selfLink));
    return selfLink.substring(selfLink.lastIndexOf("/") + 1);
  }

  /**
   * Converts a map of GCP labels into the format required to filter a request with these labels.
   *
   * @param labels A map of key:value GCP labels.
   * @return A filter string that is a concatenation of strings of the form "(labels.key eq value) "
   *     using the keys and values from labels.
   */
  public static String buildLabelsFilterString(final Map<String, String> labels) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> l : labels.entrySet()) {
      sb.append("(labels.").append(l.getKey()).append(" eq ").append(l.getValue()).append(") ");
    }
    return sb.toString().trim();
  }

  public static String buildFilterString(final Map<String, String> filters) {
    StringBuilder sb = new StringBuilder();
    Iterator<Map.Entry<String, String>> it = filters.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> f = it.next();
      sb.append(f.getKey()).append(String.format("=\"%s\" ", f.getValue()));
      if (it.hasNext()) {
        sb.append("AND ");
      }
    }
    return sb.toString().trim();
  }

  /**
   * Parses instance resource data from the specified self link URL.
   *
   * @param selfLink The self link URL to be parsed.
   * @return If successful, an {@link Optional} containing a {@link InstanceResourceData} struct
   *     containing the parsed data. Otherwise, an empty {@link Optional}.
   */
  public static Optional<InstanceResourceData> parseInstanceResourceData(final String selfLink) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(selfLink));
    Matcher matcher = INSTANCE_RESOURCE_DATA_PATTERN.matcher(selfLink);
    if (!matcher.find()) {
      return Optional.empty();
    }
    return Optional.of(
        InstanceResourceData.builder()
            .projectId(matcher.group(1))
            .zone(matcher.group(2))
            .name(matcher.group(3))
            .build());
  }
}
