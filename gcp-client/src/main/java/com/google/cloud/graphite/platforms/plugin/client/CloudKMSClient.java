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

import static com.google.cloud.graphite.platforms.plugin.client.util.ClientUtil.processResourceList;

import com.google.api.services.cloudkms.v1.model.CryptoKey;
import com.google.api.services.cloudkms.v1.model.CryptoKeyVersion;
import com.google.api.services.cloudkms.v1.model.KeyRing;
import com.google.api.services.cloudkms.v1.model.Location;
import com.google.api.services.cloudkms.v1.model.PublicKey;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import lombok.extern.java.Log;

/**
 * Client for communicating with the Cloud KMS API.
 *
 * @see <a href="https://cloud.google.com/kms">Cloud KMS</a>
 */
@Log
public class CloudKMSClient {
  private CloudKMSWrapper cloudKMS;

  CloudKMSClient(CloudKMSWrapper cloudKMS) {
    this.cloudKMS = cloudKMS;
  }

  /**
   * Retrieves a list of key {@link Location}s available in the specified project.
   *
   * @param projectId The ID of the project to check.
   * @return An list of {@link Location}s sorted by name.
   * @throws IOException An error occurred attempting to get the list of locations.
   */
  public ImmutableList<Location> listLocations(final String projectId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    return processResourceList(
        cloudKMS.listLocations(projectId), Comparator.comparing(Location::getDisplayName));
  }

  /**
   * Retrieves a list of available {@link KeyRing}s in the specified location.
   *
   * @param projectId The ID of the project to check.
   * @param location The name of the location to check.
   * @return A list of {@link KeyRing}s sorted by name
   * @throws IOException An error occurred attempting to get the list of key rings.
   */
  public ImmutableList<KeyRing> listKeyRings(final String projectId, final String location)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(location));
    return processResourceList(
        cloudKMS.listKeyRings(projectId, location), Comparator.comparing(KeyRing::getName));
  }

  /**
   * Retrieves a list of available {@link CryptoKey}s in the specified key ring.
   *
   * @param projectId The ID of the project to check.
   * @param location The name of the location to check.
   * @param keyRing The name of the key ring to check.
   * @return A list of {@link CryptoKey}s sorted by name.
   * @throws IOException An error occurred attempting to get the list of crypto keys.
   */
  public ImmutableList<CryptoKey> listCryptoKeys(
      final String projectId, final String location, final String keyRing) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(location));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(keyRing));
    return processResourceList(
        cloudKMS.listCryptoKeys(projectId, location, keyRing),
        Comparator.comparing(CryptoKey::getName));
  }

  /**
   * Retrieves a {@link CryptoKey} as specified if it exists. Call {@link CryptoKey#getPrimary()} to
   * get the primary {@link CryptoKeyVersion} for the key instead of using {@link
   * #getCryptoKeyVersion(String, String, String, String, String)} with a specified version
   * identifier.
   *
   * @param projectId The ID of the project where the key is hosted.
   * @param location The name of the location where the key is hosted.
   * @param keyRing The name of the key ring where the key is hosted.
   * @param cryptoKey The name of the key to get.
   * @return The {@link CryptoKey} specified.
   * @throws IOException An error occurred attempting to get the crypto key.
   */
  public CryptoKey getCryptoKey(
      final String projectId, final String location, final String keyRing, final String cryptoKey)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(location));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(keyRing));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cryptoKey));
    return cloudKMS.getCryptoKey(projectId, location, keyRing, cryptoKey);
  }

  /**
   * Retrieves a list of available {@link CryptoKeyVersion}s for the specified crypto key.
   *
   * @param projectId The ID of the project to check.
   * @param location The name of the location to check.
   * @param keyRing The name of the key ring to check.
   * @param cryptoKey The name of the crypto key to check.
   * @return A list of {@link CryptoKeyVersion}s sorted by name. Only returns results with the state
   *     "ENABLED".
   * @throws IOException An error occurred attempting to get the list of crypto key versions.
   */
  public ImmutableList<CryptoKeyVersion> listCryptoKeyVersions(
      final String projectId, final String location, final String keyRing, final String cryptoKey)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(location));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(keyRing));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cryptoKey));
    return processResourceList(
        cloudKMS.listCryptoKeyVersions(projectId, location, keyRing, cryptoKey),
        c -> "ENABLED".equals(c.getState()),
        Comparator.comparing(CryptoKeyVersion::getName));
  }

  /**
   * Retrieves a {@link CryptoKeyVersion} as specified if it exists.
   *
   * @param projectId The ID of the project where the key is hosted.
   * @param location The name of the location where the key is hosted.
   * @param keyRing The name of the key ring where the key is hosted.
   * @param cryptoKey The name of the key to get a specific version from.
   * @param cryptoKeyVersion The name of the version of the key to retrieve.
   * @return The {@link CryptoKeyVersion} specified.
   * @throws IOException An error occurred attempting to get the crypto key version.
   */
  public CryptoKeyVersion getCryptoKeyVersion(
      final String projectId,
      final String location,
      final String keyRing,
      final String cryptoKey,
      final String cryptoKeyVersion)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(location));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(keyRing));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cryptoKey));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cryptoKeyVersion));
    return cloudKMS.getCryptoKeyVersion(projectId, location, keyRing, cryptoKey, cryptoKeyVersion);
  }

  /**
   * Retrieves a {@link PublicKey} for the {@link CryptoKeyVersion} as specified if it exists.
   *
   * @param projectId The ID of the project where the key is hosted.
   * @param location The name of the location where the key is hosted.
   * @param keyRing The name of the key ring where the key is hosted.
   * @param cryptoKey The name of the key to get a specific version from.
   * @param cryptoKeyVersion The name of the version of the key to retrieve.
   * @return The {@link PublicKey} associated with the specified {@link CryptoKeyVersion}
   * @throws IOException An error occurred attempting to get the public key.
   */
  public PublicKey getPublicKey(
      final String projectId,
      final String location,
      final String keyRing,
      final String cryptoKey,
      final String cryptoKeyVersion)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(location));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(keyRing));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cryptoKey));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cryptoKeyVersion));
    return cloudKMS.getPublicKey(projectId, location, keyRing, cryptoKey, cryptoKeyVersion);
  }

  /**
   * Signs the provided payload with the specified key and returns the signature.
   *
   * @param projectId The ID of the project where the key is hosted.
   * @param location The name of the location where the key is hosted.
   * @param keyRing The name of the key ring where the key is hosted.
   * @param cryptoKey The name of the key to use for signing.
   * @param cryptoKeyVersion The name of version of the key to use for signing.
   * @param payload The plain string payload to be signed. It will be converted into a base64
   *     encoded digest using the algorithm corresponding to the specified key.
   * @return The signature produced by signing the payload with the specified key.
   * @throws IOException An error occurred attempting to get the signature.
   */
  public String asymmetricSign(
      final String projectId,
      final String location,
      final String keyRing,
      final String cryptoKey,
      final String cryptoKeyVersion,
      final String payload)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(location));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(keyRing));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cryptoKey));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cryptoKeyVersion));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(payload));
    CryptoKeyVersion cryptoKeyVersionObj =
        cloudKMS.getCryptoKeyVersion(projectId, location, keyRing, cryptoKey, cryptoKeyVersion);
    String purpose = parseKeyPurpose(cryptoKeyVersionObj.getAlgorithm());
    Preconditions.checkArgument(
        "SIGN".equals(purpose), "Key specified should have purpose SIGN, instead was: %s", purpose);
    String digestAlgorithm = parseDigestAlgorithm(cryptoKeyVersionObj.getAlgorithm());
    try {
      return cloudKMS.asymmetricSign(
          projectId, location, keyRing, cryptoKey, cryptoKeyVersion, digestAlgorithm, payload);
    } catch (NoSuchAlgorithmException nsae) {
      throw new IllegalStateException(
          "The digest algorithm was verified but still caught NoSuchAlgorithmException.", nsae);
    }
  }

  /*
   * Below two methods use the definition of the enum type provided in the docs:
   * https://cloud.google.com/kms/docs/reference/rest/v1/CryptoKeyVersionAlgorithm
   */
  private static String parseKeyPurpose(String algorithm) {
    String[] tokens = algorithm.split("_");
    if (tokens.length < 3 || tokens.length > 5) {
      throw new IllegalArgumentException("Invalid key algorithm provided: " + algorithm);
    }
    switch (tokens[1]) {
      case "SIGN":
      case "DECRYPT":
        return tokens[1];
      case "KEY":
      case "SYMMETRIC":
        throw new IllegalArgumentException("Key with unspecified purpose provided: " + algorithm);
      default:
        throw new IllegalArgumentException("Invalid key algorithm provided: " + algorithm);
    }
  }

  private static String parseDigestAlgorithm(String algorithm) {
    String[] tokens = algorithm.split("_");
    if (tokens.length < 3 || tokens.length > 5) {
      throw new IllegalArgumentException();
    }
    String digestAlgorithm = tokens[tokens.length - 1];
    switch (digestAlgorithm) {
      case "SHA256":
      case "SHA384":
      case "SHA512":
        // Return the digest algorithm in a form compatible with MessageDigest.getInstance()
        return "SHA-" + digestAlgorithm.substring(3);
      case "ENCRYPTION":
      case "UNSPECIFIED":
        throw new IllegalArgumentException(
            "Key with unspecified digest algorithm provided: " + algorithm);
      default:
        throw new IllegalArgumentException("Invalid key algorithm provided: " + algorithm);
    }
  }
}
