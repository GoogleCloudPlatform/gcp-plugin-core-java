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

import static org.junit.Assert.assertNotNull;

import com.google.api.client.http.HttpRequestInitializer;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests {@link com.google.cloud.graphite.platforms.plugin.client.ClientFactory}. */
@RunWith(MockitoJUnitRunner.class)
public class ClientFactoryTest {
  @Test
  public void defaultTransport() throws Exception {
    HttpRequestInitializer httpRequestInitializer = Mockito.mock(HttpRequestInitializer.class);
    ClientFactory cf =
        new ClientFactory(Optional.empty(), httpRequestInitializer, "TEST_APPLICATION_NAME");
    assertNotNull(cf.containerClient());
    assertNotNull(cf.cloudResourceManagerClient());
    assertNotNull(cf.computeClient());
    assertNotNull(cf.binaryAuthorizationClient());
    assertNotNull(cf.cloudKMSClient());
    assertNotNull(cf.containerAnalysisClient());
  }
}
