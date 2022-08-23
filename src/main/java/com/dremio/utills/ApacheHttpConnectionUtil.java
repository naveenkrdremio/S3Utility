/*
 * Copyright (C) 2017-2019 Dremio Corporation
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

package com.dremio.utills;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.s3.S3Configs;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

/**
 * Apache HTTP Connection Utility that supports aws sdk 2.X
 */
public final class ApacheHttpConnectionUtil {
  private static final Logger logger = LoggerFactory.getLogger(ApacheHttpConnectionUtil.class);

  private ApacheHttpConnectionUtil() {
  }

  public static SdkHttpClient.Builder<?> initConnectionSettings(S3Configs conf) {
    final ApacheHttpClient.Builder httpBuilder = ApacheHttpClient.builder();
    httpBuilder.maxConnections(1500);
    httpBuilder.connectionTimeout(
            Duration.ofSeconds(100));
    httpBuilder.socketTimeout(
            Duration.ofSeconds(100));

    return httpBuilder;
  }
}
