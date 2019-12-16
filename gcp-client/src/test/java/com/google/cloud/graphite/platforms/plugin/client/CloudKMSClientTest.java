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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;

import com.google.api.services.cloudkms.v1.model.CryptoKey;
import com.google.api.services.cloudkms.v1.model.CryptoKeyVersion;
import com.google.api.services.cloudkms.v1.model.KeyRing;
import com.google.api.services.cloudkms.v1.model.Location;
import com.google.api.services.cloudkms.v1.model.PublicKey;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests {@link CloudKMSClient}. */
@RunWith(MockitoJUnitRunner.class)
public class CloudKMSClientTest {
  private static final String TEST_PROJECT_ID = "test-project";
  private static final String TEST_LOCATION = "test-location";
  private static final String OTHER_LOCATION = "other-location";
  private static final String TEST_KEY_RING = "test-key-ring";
  private static final String OTHER_KEY_RING = "other-key-ring";
  private static final String TEST_CRYPTO_KEY = "test-crypto-key";
  private static final String OTHER_CRYPTO_KEY = "other-crypto-key";
  private static final String TEST_CRYPTO_KEY_VERSION = "test-crypto-key-version";
  private static final String OTHER_CRYPTO_KEY_VERSION = "other-crypto-key-version";
  private static final String TEST_PAYLOAD = "test-payload";
  private static final String TEST_SIGNATURE = "test-signature";
  private static final String PUBLIC_KEY_ALGORITHM = "RSA_SIGN_PKCS1_4096_SHA512";

  @Test(expected = IllegalArgumentException.class)
  public void testListLocationsErrorWithNullProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listLocations(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListLocationsErrorWithEmptyProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listLocations("");
  }

  @Test(expected = IOException.class)
  public void testListLocationsErrorWithIOException() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(cloudKMS.listLocations(anyString())).thenThrow(IOException.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listLocations(TEST_PROJECT_ID);
  }

  @Test
  public void testListLocations() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(cloudKMS.listLocations(TEST_PROJECT_ID))
        .thenReturn(initLocationList(ImmutableList.of(TEST_LOCATION, OTHER_LOCATION)));
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    List<Location> expected = initLocationList(ImmutableList.of(OTHER_LOCATION, TEST_LOCATION));
    List<Location> actual = cloudKMSClient.listLocations(TEST_PROJECT_ID);
    assertNotNull(actual);
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListKeyRingsErrorWithNullProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listKeyRings(null, TEST_LOCATION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListKeyRingsErrorWithEmptyProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listKeyRings("", TEST_LOCATION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListKeyRingsErrorWithNullLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listKeyRings(TEST_PROJECT_ID, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListKeyRingsErrorWithEmptyLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listKeyRings(TEST_PROJECT_ID, "");
  }

  @Test(expected = IOException.class)
  public void testListKeyRingsErrorWithIOException() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(cloudKMS.listKeyRings(anyString(), anyString())).thenThrow(IOException.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listKeyRings(TEST_PROJECT_ID, TEST_LOCATION);
  }

  @Test
  public void testListKeyRings() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(cloudKMS.listKeyRings(TEST_PROJECT_ID, TEST_LOCATION))
        .thenReturn(initKeyRingList(ImmutableList.of(TEST_KEY_RING, OTHER_KEY_RING)));
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    List<KeyRing> expected = initKeyRingList(ImmutableList.of(OTHER_KEY_RING, TEST_KEY_RING));
    List<KeyRing> actual = cloudKMSClient.listKeyRings(TEST_PROJECT_ID, TEST_LOCATION);
    assertNotNull(actual);
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeysErrorWithNullProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeys(null, TEST_LOCATION, TEST_KEY_RING);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeysErrorWithEmptyProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeys("", TEST_LOCATION, TEST_KEY_RING);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeysErrorWithNullLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeys(TEST_PROJECT_ID, null, TEST_KEY_RING);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeysErrorWithEmptyLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeys(TEST_PROJECT_ID, "", TEST_KEY_RING);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeysErrorWithNullKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeys(TEST_PROJECT_ID, TEST_LOCATION, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeysErrorWithEmptyKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeys(TEST_PROJECT_ID, TEST_LOCATION, "");
  }

  @Test(expected = IOException.class)
  public void testListCryptoKeysErrorWithIOException() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(cloudKMS.listCryptoKeys(anyString(), anyString(), anyString()))
        .thenThrow(IOException.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeys(TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING);
  }

  @Test
  public void testListCryptoKeys() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(cloudKMS.listCryptoKeys(TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING))
        .thenReturn(initCryptoKeyList(ImmutableList.of(TEST_CRYPTO_KEY, OTHER_CRYPTO_KEY)));
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    List<CryptoKey> expected =
        initCryptoKeyList(ImmutableList.of(OTHER_CRYPTO_KEY, TEST_CRYPTO_KEY));
    List<CryptoKey> actual =
        cloudKMSClient.listCryptoKeys(TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING);
    assertNotNull(actual);
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyErrorWithNullProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKey(null, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyErrorWithEmptyProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKey("", TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyErrorWithNullLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKey(TEST_PROJECT_ID, null, TEST_KEY_RING, TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyErrorWithEmptyLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKey(TEST_PROJECT_ID, "", TEST_KEY_RING, TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyErrorWithNullKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKey(TEST_PROJECT_ID, TEST_LOCATION, null, TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyErrorWithEmptyKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKey(TEST_PROJECT_ID, TEST_LOCATION, "", TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyErrorWithNullCryptoKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKey(TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyErrorWithEmptyCryptoKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKey(TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, "");
  }

  @Test(expected = IOException.class)
  public void testGetCryptoKeyErrorWithIOException() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(cloudKMS.getCryptoKey(anyString(), anyString(), anyString(), anyString()))
        .thenThrow(IOException.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKey(TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY);
  }

  @Test
  public void testGetCryptoKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(
            cloudKMS.getCryptoKey(TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY))
        .thenReturn(new CryptoKey().setName(TEST_CRYPTO_KEY));
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    CryptoKey expected = new CryptoKey().setName(TEST_CRYPTO_KEY);
    CryptoKey actual =
        cloudKMSClient.getCryptoKey(TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY);
    assertNotNull(actual);
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeyVersionsErrorWithNullProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeyVersions(null, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeyVersionsErrorWithEmptyProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeyVersions("", TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeyVersionsErrorWithNullLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeyVersions(TEST_PROJECT_ID, null, TEST_KEY_RING, TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeyVersionsErrorWithEmptyLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeyVersions(TEST_PROJECT_ID, "", TEST_KEY_RING, TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeyVersionsErrorWithNullKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeyVersions(TEST_PROJECT_ID, TEST_LOCATION, null, TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeyVersionsErrorWithEmptyKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeyVersions(TEST_PROJECT_ID, TEST_LOCATION, "", TEST_CRYPTO_KEY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeyVersionsErrorWithNullCryptoKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeyVersions(TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListCryptoKeyVersionsErrorWithEmptyCryptoKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeyVersions(TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, "");
  }

  @Test(expected = IOException.class)
  public void testListCryptoKeyVersionsErrorWithIOException() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(cloudKMS.listCryptoKeyVersions(anyString(), anyString(), anyString(), anyString()))
        .thenThrow(IOException.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.listCryptoKeyVersions(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY);
  }

  @Test
  public void testListCryptoKeyVersions() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(
            cloudKMS.listCryptoKeyVersions(
                TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY))
        .thenReturn(
            initCryptoKeyVersionList(
                ImmutableList.of(TEST_CRYPTO_KEY_VERSION, OTHER_CRYPTO_KEY_VERSION)));
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    List<CryptoKeyVersion> expected =
        initCryptoKeyVersionList(
            ImmutableList.of(OTHER_CRYPTO_KEY_VERSION, TEST_CRYPTO_KEY_VERSION));
    List<CryptoKeyVersion> actual =
        cloudKMSClient.listCryptoKeyVersions(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY);
    assertNotNull(actual);
    assertEquals(expected, actual);
  }

  @Test
  public void testListCryptoKeyVersionsOmitsDisabledKeys() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(
            cloudKMS.listCryptoKeyVersions(
                TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY))
        .thenReturn(
            initCryptoKeyVersionList(
                ImmutableList.of(TEST_CRYPTO_KEY_VERSION, OTHER_CRYPTO_KEY_VERSION),
                ImmutableList.of("ENABLED", "DISABLED")));
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    List<CryptoKeyVersion> expected =
        initCryptoKeyVersionList(
            ImmutableList.of(TEST_CRYPTO_KEY_VERSION), ImmutableList.of("ENABLED"));
    List<CryptoKeyVersion> actual =
        cloudKMSClient.listCryptoKeyVersions(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY);
    assertNotNull(actual);
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyVersionErrorWithNullProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKeyVersion(
        null, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyVersionErrorWithEmptyProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKeyVersion(
        "", TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyVersionErrorWithNullLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKeyVersion(
        TEST_PROJECT_ID, null, TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyVersionErrorWithEmptyLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKeyVersion(
        TEST_PROJECT_ID, "", TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyVersionErrorWithNullKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKeyVersion(
        TEST_PROJECT_ID, TEST_LOCATION, null, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyVersionErrorWithEmptyKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKeyVersion(
        TEST_PROJECT_ID, TEST_LOCATION, "", TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyVersionErrorWithNullCryptoKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKeyVersion(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, null, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyVersionErrorWithEmptyCryptoKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKeyVersion(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, "", TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyVersionErrorWithNullCryptoKeyVersion() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKeyVersion(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCryptoKeyVersionErrorWithEmptyCryptoKeyVersion() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKeyVersion(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, "");
  }

  @Test(expected = IOException.class)
  public void testGetCryptoKeyVersionErrorWithIOException() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(
            cloudKMS.getCryptoKeyVersion(
                anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenThrow(IOException.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getCryptoKeyVersion(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test
  public void testGetCryptoKeyVersion() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(
            cloudKMS.getCryptoKeyVersion(
                TEST_PROJECT_ID,
                TEST_LOCATION,
                TEST_KEY_RING,
                TEST_CRYPTO_KEY,
                TEST_CRYPTO_KEY_VERSION))
        .thenReturn(new CryptoKeyVersion().setName(TEST_CRYPTO_KEY_VERSION));
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    CryptoKeyVersion expected = new CryptoKeyVersion().setName(TEST_CRYPTO_KEY_VERSION);
    CryptoKeyVersion actual =
        cloudKMSClient.getCryptoKeyVersion(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_KEY_RING,
            TEST_CRYPTO_KEY,
            TEST_CRYPTO_KEY_VERSION);
    assertNotNull(actual);
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPublicKeyErrorWithNullProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getPublicKey(
        null, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPublicKeyErrorWithEmptyProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getPublicKey(
        "", TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPublicKeyErrorWithNullLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getPublicKey(
        TEST_PROJECT_ID, null, TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPublicKeyErrorWithEmptyLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getPublicKey(
        TEST_PROJECT_ID, "", TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPublicKeyErrorWithNullKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getPublicKey(
        TEST_PROJECT_ID, TEST_LOCATION, null, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPublicKeyErrorWithEmptyKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getPublicKey(
        TEST_PROJECT_ID, TEST_LOCATION, "", TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPublicKeyErrorWithNullCryptoKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getPublicKey(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, null, TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPublicKeyErrorWithEmptyCryptoKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getPublicKey(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, "", TEST_CRYPTO_KEY_VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPublicKeyErrorWithNullCryptoKeyVersion() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getPublicKey(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPublicKeyErrorWithEmptyCryptoKeyVersion() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getPublicKey(TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, "");
  }

  @Test(expected = IOException.class)
  public void testGetPublicKeyErrorWithIOException() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(
            cloudKMS.getPublicKey(anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenThrow(IOException.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.getPublicKey(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION);
  }

  @Test
  public void testGetPublicKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(
            cloudKMS.getPublicKey(
                TEST_PROJECT_ID,
                TEST_LOCATION,
                TEST_KEY_RING,
                TEST_CRYPTO_KEY,
                TEST_CRYPTO_KEY_VERSION))
        .thenReturn(new PublicKey().setAlgorithm(PUBLIC_KEY_ALGORITHM));
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    PublicKey expected = new PublicKey().setAlgorithm(PUBLIC_KEY_ALGORITHM);
    PublicKey actual =
        cloudKMSClient.getPublicKey(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_KEY_RING,
            TEST_CRYPTO_KEY,
            TEST_CRYPTO_KEY_VERSION);
    assertNotNull(actual);
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithNullProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        null, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION, TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithEmptyProjectId() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        "", TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION, TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithNullLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        TEST_PROJECT_ID,
        null,
        TEST_KEY_RING,
        TEST_CRYPTO_KEY,
        TEST_CRYPTO_KEY_VERSION,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithEmptyLocation() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        TEST_PROJECT_ID, "", TEST_KEY_RING, TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION, TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithNullKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        TEST_PROJECT_ID,
        TEST_LOCATION,
        null,
        TEST_CRYPTO_KEY,
        TEST_CRYPTO_KEY_VERSION,
        TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithEmptyKeyRing() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        TEST_PROJECT_ID, TEST_LOCATION, "", TEST_CRYPTO_KEY, TEST_CRYPTO_KEY_VERSION, TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithNullCryptoKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, null, TEST_CRYPTO_KEY_VERSION, TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithEmptyCryptoKey() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, "", TEST_CRYPTO_KEY_VERSION, TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithNullCryptoKeyVersion() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, null, TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithEmptyCryptoKeyVersion() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, "", TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithNullPayload() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, null, TEST_PAYLOAD);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAsymmetricSignErrorWithEmptyPayload() throws IOException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        TEST_PROJECT_ID, TEST_LOCATION, TEST_KEY_RING, TEST_CRYPTO_KEY, "", TEST_PAYLOAD);
  }

  @Test(expected = IOException.class)
  public void testAsymmetricSignErrorWithIOException()
      throws IOException, NoSuchAlgorithmException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(
            cloudKMS.getCryptoKeyVersion(
                anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(new CryptoKeyVersion().setAlgorithm(PUBLIC_KEY_ALGORITHM));
    Mockito.when(
            cloudKMS.asymmetricSign(
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyString()))
        .thenThrow(IOException.class);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    cloudKMSClient.asymmetricSign(
        TEST_PROJECT_ID,
        TEST_LOCATION,
        TEST_KEY_RING,
        TEST_CRYPTO_KEY,
        TEST_CRYPTO_KEY_VERSION,
        TEST_PAYLOAD);
  }

  @Test
  public void testAsymmetricSign() throws IOException, NoSuchAlgorithmException {
    CloudKMSWrapper cloudKMS = Mockito.mock(CloudKMSWrapper.class);
    Mockito.when(
            cloudKMS.getCryptoKeyVersion(
                anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(new CryptoKeyVersion().setAlgorithm("RSA_SIGN_PKCS1_4096_SHA512"));
    Mockito.when(
            cloudKMS.asymmetricSign(
                TEST_PROJECT_ID,
                TEST_LOCATION,
                TEST_KEY_RING,
                TEST_CRYPTO_KEY,
                TEST_CRYPTO_KEY_VERSION,
                "SHA-512",
                TEST_PAYLOAD))
        .thenReturn(TEST_SIGNATURE);
    CloudKMSClient cloudKMSClient = new CloudKMSClient(cloudKMS);
    String result =
        cloudKMSClient.asymmetricSign(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_KEY_RING,
            TEST_CRYPTO_KEY,
            TEST_CRYPTO_KEY_VERSION,
            TEST_PAYLOAD);
    assertEquals(TEST_SIGNATURE, result);
  }

  private static ImmutableList<Location> initLocationList(List<String> names) {
    return ImmutableList.copyOf(
        names.stream().map(name -> new Location().setName(name)).collect(Collectors.toList()));
  }

  private static ImmutableList<KeyRing> initKeyRingList(List<String> names) {
    return ImmutableList.copyOf(
        names.stream().map(name -> new KeyRing().setName(name)).collect(Collectors.toList()));
  }

  private static ImmutableList<CryptoKey> initCryptoKeyList(List<String> names) {
    return ImmutableList.copyOf(
        names.stream().map(name -> new CryptoKey().setName(name)).collect(Collectors.toList()));
  }

  private static ImmutableList<CryptoKeyVersion> initCryptoKeyVersionList(List<String> names) {
    List<String> states = names.stream().map(name -> "ENABLED").collect(Collectors.toList());
    return initCryptoKeyVersionList(names, states);
  }

  private static ImmutableList<CryptoKeyVersion> initCryptoKeyVersionList(
      List<String> names, List<String> states) {
    assertEquals(names.size(), states.size());
    return ImmutableList.copyOf(
        IntStream.range(0, names.size())
            .boxed()
            .map(i -> new CryptoKeyVersion().setName(names.get(i)).setState(states.get(i)))
            .collect(Collectors.toList()));
  }
}
