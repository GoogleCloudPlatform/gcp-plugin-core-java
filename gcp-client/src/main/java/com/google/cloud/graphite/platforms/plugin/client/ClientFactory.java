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

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.compute.Compute;
import com.google.api.services.container.Container;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;

/**
 * A factory for generating clients which provide convenience methods for the GCP client libraries.
 */
public class ClientFactory {
  private final HttpTransport transport;
  private final JsonFactory jsonFactory;
  private final HttpRequestInitializer httpRequestInitializer;
  private final String applicationName;

  /**
   * Constructor for {@link ClientFactory}.
   *
   * @param httpTransport An optional HTTP Transport for making HTTP requests. If not specified, the
   *     default trusted NetHttpTransport will be generated.
   * @param httpRequestInitializer Used to initialize HTTP requests, and must contain the Credential
   *     for authenticating requests.
   * @param applicationName The name of the application which is using the clients in order to tie
   *     this information to requests to track usage.
   * @throws IOException If generating a new trusted HTTP Transport failed
   * @throws GeneralSecurityException If generating a new trusted HTTP Transport failed due to
   */
  public ClientFactory(
      final Optional<HttpTransport> httpTransport,
      final HttpRequestInitializer httpRequestInitializer,
      final String applicationName)
      throws IOException, GeneralSecurityException {
    this.transport = httpTransport.orElse(GoogleNetHttpTransport.newTrustedTransport());
    this.jsonFactory = new JacksonFactory();
    this.httpRequestInitializer = Preconditions.checkNotNull(httpRequestInitializer);
    this.applicationName = Preconditions.checkNotNull(applicationName);
  }

  /**
   * Initializes a {@link ComputeClient} with the properties of this {@link ClientFactory}.
   *
   * @return A {@link ComputeClient} for interacting with the Google Compute Engine API.
   */
  public ComputeClient computeClient() {
    return new ComputeClient(
        new ComputeWrapper(
            new Compute.Builder(transport, jsonFactory, httpRequestInitializer)
                .setGoogleClientRequestInitializer(this::initializeRequest)
                .setApplicationName(applicationName)
                .build()));
  }

  /**
   * Initializes a {@link CloudResourceManagerClient} with the properties of this {@link
   * ClientFactory}.
   *
   * @return A {@link CloudResourceManagerClient} for interacting with the Cloud Resource Manger
   *     API.
   */
  public CloudResourceManagerClient cloudResourceManagerClient() {
    return new CloudResourceManagerClient(
        new CloudResourceManagerWrapper(
            new CloudResourceManager.Builder(transport, jsonFactory, httpRequestInitializer)
                .setGoogleClientRequestInitializer(this::initializeRequest)
                .setApplicationName(applicationName)
                .build()));
  }

  /**
   * Initializes a {@link ContainerClient} with the properties of this {@link ClientFactory}.
   *
   * @return A {@link ContainerClient} for interacting with the Google Kubernetes Engine API.
   */
  public ContainerClient containerClient() {
    return new ContainerClient(
        new ContainerWrapper(
            new Container.Builder(transport, jsonFactory, httpRequestInitializer)
                .setGoogleClientRequestInitializer(this::initializeRequest)
                .setApplicationName(applicationName)
                .build()));
  }

  private void initializeRequest(final AbstractGoogleClientRequest request) {
    request.setRequestHeaders(request.getRequestHeaders().setUserAgent(applicationName));
  }
}
