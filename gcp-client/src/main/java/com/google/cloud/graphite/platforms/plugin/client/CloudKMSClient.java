package com.google.cloud.graphite.platforms.plugin.client;

public class CloudKMSClient {
  private CloudKMSWrapper cloudKMS;

  CloudKMSClient(CloudKMSWrapper cloudKMS) {
    this.cloudKMS = cloudKMS;
  }
}
