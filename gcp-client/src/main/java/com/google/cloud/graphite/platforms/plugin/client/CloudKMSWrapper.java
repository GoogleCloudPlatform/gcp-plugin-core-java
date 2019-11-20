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

import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.model.AsymmetricSignRequest;
import com.google.api.services.cloudkms.v1.model.CryptoKey;
import com.google.api.services.cloudkms.v1.model.CryptoKeyVersion;
import com.google.api.services.cloudkms.v1.model.Digest;
import com.google.api.services.cloudkms.v1.model.KeyRing;
import com.google.api.services.cloudkms.v1.model.Location;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

class CloudKMSWrapper {
  private CloudKMS cloudKMS;

  CloudKMSWrapper(CloudKMS cloudKMS) {
    this.cloudKMS = cloudKMS;
  }

  List<Location> listLocations(String projectId) throws IOException {
    return cloudKMS.projects().locations().list(toLocationName(projectId)).execute().getLocations();
  }

  List<KeyRing> listKeyRings(String projectId, String location) throws IOException {
    return cloudKMS
        .projects()
        .locations()
        .keyRings()
        .list(toKeyRingParent(projectId, location))
        .execute()
        .getKeyRings();
  }

  List<CryptoKey> listCryptoKeys(String projectId, String location, String keyRing)
      throws IOException {
    return cloudKMS
        .projects()
        .locations()
        .keyRings()
        .cryptoKeys()
        .list(toCryptoKeyParent(projectId, location, keyRing))
        .execute()
        .getCryptoKeys();
  }

  List<CryptoKeyVersion> listCryptoKeyVersions(
      String projectId, String location, String keyRing, String cryptoKey) throws IOException {
    return cloudKMS
        .projects()
        .locations()
        .keyRings()
        .cryptoKeys()
        .cryptoKeyVersions()
        .list(toCryptoKeyVersionParent(projectId, location, keyRing, cryptoKey))
        .execute()
        .getCryptoKeyVersions();
  }

  CryptoKeyVersion getCryptoKeyVersion(
      String projectId, String location, String keyRing, String cryptoKey, String cryptoKeyVersion)
      throws IOException {
    return cloudKMS
        .projects()
        .locations()
        .keyRings()
        .cryptoKeys()
        .cryptoKeyVersions()
        .get(toCryptoKeyVersionName(projectId, location, keyRing, cryptoKey, cryptoKeyVersion))
        .execute();
  }

  String asymmetricSign(
      String projectId,
      String location,
      String keyRing,
      String cryptoKey,
      String cryptoKeyVersion,
      String algorithm,
      String payload)
      throws IOException, NoSuchAlgorithmException {
    return cloudKMS
        .projects()
        .locations()
        .keyRings()
        .cryptoKeys()
        .cryptoKeyVersions()
        .asymmetricSign(
            toCryptoKeyVersionName(projectId, location, keyRing, cryptoKey, cryptoKeyVersion),
            new AsymmetricSignRequest().setDigest(toDigest(algorithm, payload)))
        .execute()
        .getSignature();
  }

  private static String toLocationName(final String projectId) {
    return String.format("projects/%s", projectId);
  }

  private static String toKeyRingParent(final String projectId, final String location) {
    return String.format("projects/%s/locations/%s", projectId, location);
  }

  private static String toCryptoKeyParent(
      final String projectId, final String location, final String keyRing) {
    return String.format("projects/%s/locations/%s/keyRings/%s", projectId, location, keyRing);
  }

  private static String toCryptoKeyVersionParent(
      final String projectId, final String location, final String keyRing, final String cryptoKey) {
    return String.format(
        "projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
        projectId, location, keyRing, cryptoKey);
  }

  private static String toCryptoKeyVersionName(
      final String projectId,
      final String location,
      final String keyRing,
      final String cryptoKey,
      final String cryptoKeyVersion) {
    return String.format(
        "projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s/cryptoKeyVersions/%s",
        projectId, location, keyRing, cryptoKey, cryptoKeyVersion);
  }

  private static Digest toDigest(final String algorithm, final String payload)
      throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance(algorithm);
    byte[] digest = md.digest(payload.getBytes());
    Digest result = new Digest();
    switch (algorithm) {
      case "SHA-256":
        return result.encodeSha256(digest);
      case "SHA-384":
        return result.encodeSha384(digest);
      case "SHA-512":
        return result.encodeSha512(digest);
      default:
        throw new IllegalArgumentException(
            "algorithm should be one of SHA-256, SHA-384, or SHA-512 but was: " + algorithm);
    }
  }
}
