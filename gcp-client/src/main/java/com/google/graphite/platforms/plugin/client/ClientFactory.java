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

package com.google.graphite.platforms.plugin.client;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.compute.Compute;
import com.google.api.services.container.Container;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;

public class ClientFactory {
  private final HttpTransport transport;
  private final JsonFactory jsonFactory;
  private final HttpRequestInitializer httpRequestInitializer;
  private final String applicationName;

  public ClientFactory(
      Optional<HttpTransport> httpTransport,
      HttpRequestInitializer httpRequestInitializer,
      String applicationName)
      throws IOException, GeneralSecurityException {
    this.transport = httpTransport.orElse(GoogleNetHttpTransport.newTrustedTransport());
    this.jsonFactory = new JacksonFactory();
    this.httpRequestInitializer = httpRequestInitializer;
    this.applicationName = applicationName;
  }

  public ComputeClient computeClient() {
    return new ComputeClient(
        new Compute.Builder(transport, jsonFactory, httpRequestInitializer)
            .setGoogleClientRequestInitializer(
                request ->
                    request.setRequestHeaders(
                        request.getRequestHeaders().setUserAgent(applicationName)))
            .setApplicationName(applicationName)
            .build());
  }

  public CloudResourceManagerClient cloudResourceManagerClient() {
    return new CloudResourceManagerClient(
        new CloudResourceManager.Builder(transport, jsonFactory, httpRequestInitializer)
            .setGoogleClientRequestInitializer(
                request ->
                    request.setRequestHeaders(
                        request.getRequestHeaders().setUserAgent(applicationName)))
            .setApplicationName(applicationName)
            .build());
  }

  public ContainerClient containerClient() {
    return new ContainerClient(
        new Container.Builder(transport, jsonFactory, httpRequestInitializer)
            .setGoogleClientRequestInitializer(
                request ->
                    request.setRequestHeaders(
                        request.getRequestHeaders().setUserAgent(applicationName)))
            .setApplicationName(applicationName)
            .build());
  }
}
