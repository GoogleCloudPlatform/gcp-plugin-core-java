<!--
 Copyright 2019 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 compliance with the License. You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software distributed under the License
 is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing permissions and limitations under the
 License.
-->
# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0) and this project
adheres to [Sematic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unresolved]

 ### Security
 
 ### Added
 
 ### Changed
 
 ### Removed
 
 ### Fixed
 
## [0.2.0] - 2019-10-28

 ### Changed
  - com.google.http-client:google-http-client version changed: 1.24.1 to 1.21.0
  - com.google.http-client:google-http-client-jackson2 version changed: 1.24.1 to 1.25.0
  - com.google.api-client:api-client version changed: 1.24.1 to 1.25.0
  - com.google.guava version changed: 14.0.1 to 20.0
  - com.google.api-services versions:
     - google-api-services-compute changed: v1-rev213-1.24.1 to v1-rev214-1.25.0
     - google-api-services-container changed: v1-rev74-1.24.1 to v1-rev74-1.25.0
     - google-api-services-cloudresourcemanager changed: v1-rev547-1.24.1 to v1-rev547-1.25.0
 
 ### Added
  - Integrated repository into team CI server.

## [0.1.2] - 2019-09-05

 ### Fixed
  - simulateMaintenanceEvent() in ComputeClient was just returning the request object. Now executes
  and returns the operation to be able to track completion.

 
## [0.1.1] - 2019-09-03
 
 ### Changed
  - Issue #3: Group ID changed from com.google.graphite to com.google.cloud.graphite
  - com.google.http-client:google-http-client version changed: 1.29.2 to 1.24.1
  - com.google.api-client:api-client version changed: 1.29.2 to 1.24.1
  - com.google.guava:guava version changed: 19.0 to 14.0.1
  - com.google.api-services versions:
     - google-api-services-compute changed: v1-rev20190624-1.29.2 to v1-rev213-1.24.1
     - google-api-services-container changed: v1-rev20190628-1.29.2 to v1-rev74-1.24.1
     - google-api-services-cloudresourcemanager changed: v1-rev20190807-1.29.2 to v1-rev547-1.24.1
 
## [0.1.0] - 2019-08-26
 
 ### Added
  - Module `gcp-client` for the shared convenience libraries for GCP used by the Jenkins plugins.