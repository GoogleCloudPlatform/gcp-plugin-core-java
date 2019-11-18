package com.google.cloud.graphite.platforms.plugin.client;

import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.model.AsymmetricSignRequest;
import com.google.api.services.cloudkms.v1.model.CryptoKey;
import com.google.api.services.cloudkms.v1.model.CryptoKeyVersion;
import com.google.api.services.cloudkms.v1.model.Digest;
import com.google.api.services.cloudkms.v1.model.KeyRing;
import com.google.api.services.cloudkms.v1.model.Location;
import java.io.IOException;
import java.util.Base64;
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
      String digest)
      throws IOException {
    return cloudKMS
        .projects()
        .locations()
        .keyRings()
        .cryptoKeys()
        .cryptoKeyVersions()
        .asymmetricSign(
            toCryptoKeyVersionName(projectId, location, keyRing, cryptoKey, cryptoKeyVersion),
            toAsymmetricSignRequest(digest))
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

  private static AsymmetricSignRequest toAsymmetricSignRequest(final String digest) {
    return new AsymmetricSignRequest()
        .setDigest(new Digest().setSha512(Base64.getEncoder().encodeToString(digest.getBytes())));
  }
}
