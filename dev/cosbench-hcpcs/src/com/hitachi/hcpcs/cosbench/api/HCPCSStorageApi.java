/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Vantara, 2019. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * ========================================================================
 */
package com.hitachi.hcpcs.cosbench.api;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.DnsResolver;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.SkipMd5CheckStrategy;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;
import com.hitachi.hcpcs.cosbench.resolver.DynamicRRDnsResolver;
import com.hitachi.hcpcs.cosbench.resolver.DynamicRRMarathonResolver;
import com.hitachi.hcpcs.cosbench.resolver.StaticRRDnsResolver;
import com.intel.cosbench.api.storage.NoneStorage;
import com.intel.cosbench.api.storage.StorageException;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.Logger;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

public class HCPCSStorageApi extends NoneStorage {

    private static final String MARATHON_DEFAULT_SCHEME = "http";
    private static final String MARATHON_DEFAULT_PORT = "8080";
    private static final String HCPCS_S3_APP_ID = "clientaccess.data";
    private static final int RESOLVER_PERIOD = 30;
    private static final TimeUnit RESOLVER_UNIT = TimeUnit.SECONDS;

    private AmazonS3 s3Client;

    HCPCSStorageApi() {
    }

    @Override
    public void init(Config config, Logger logger) {
        synchronized (this) {
            if (s3Client != null) {
                return;
            }
            super.init(config, logger);

            // sets global properties
            String insecure = config.get(HCPCSOptions.INSECURE.param(),
                                         HCPCSOptions.INSECURE.value());
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY,
                               insecure);
            logParameter(HCPCSOptions.INSECURE.param(), insecure);

            String md5Disable = config.get(HCPCSOptions.MD5_DISABLE.param(),
                                           HCPCSOptions.MD5_DISABLE.value());
            logParameter(HCPCSOptions.MD5_DISABLE.param(), md5Disable);
            System.setProperty(SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY,
                               md5Disable);
            System.setProperty(SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY,
                               md5Disable);

            // handle options that have default values
            int maxConnections = config.getInt(HCPCSOptions.MAX_CONNECTIONS.param(),
                                               Integer.valueOf(HCPCSOptions.MAX_CONNECTIONS
                                                       .value()));
            logParameter(HCPCSOptions.MAX_CONNECTIONS.param(), Integer.toString(maxConnections));
            int maxErrorRetry = config.getInt(HCPCSOptions.MAX_ERROR_RETRY.param(),
                                              Integer.valueOf(HCPCSOptions.MAX_ERROR_RETRY
                                                      .value()));
            logParameter(HCPCSOptions.MAX_ERROR_RETRY.param(), Integer.toString(maxErrorRetry));
            boolean pathStyleAccess = config.getBoolean(HCPCSOptions.PATH_STYLE_ACCESS.param(),
                                                        Boolean.valueOf(HCPCSOptions.PATH_STYLE_ACCESS
                                                                .value()));
            logParameter(HCPCSOptions.PATH_STYLE_ACCESS.param(), Boolean.toString(pathStyleAccess));
            String region = config.get(HCPCSOptions.REGION.param(), HCPCSOptions.REGION.value());
            logParameter(HCPCSOptions.REGION.param(), region);

            String resolver = config.get(HCPCSOptions.RESOLVER.param(),
                                         HCPCSOptions.RESOLVER.value());
            logParameter(HCPCSOptions.RESOLVER.param(), resolver);

            String endpoint = config.get(HCPCSOptions.ENDPOINT.param(),
                                         HCPCSOptions.ENDPOINT.value());
            logParameter(HCPCSOptions.ENDPOINT.param(), endpoint);

            String accessKey = config.get(HCPCSOptions.ACCESS_KEY.param(),
                                          HCPCSOptions.ACCESS_KEY.value());
            logParameter(HCPCSOptions.ACCESS_KEY.param(),
                         (StringUtils.isNullOrEmpty(accessKey) ? "is unset" : "is set"));

            String secretKey = config.get(HCPCSOptions.SECRET_KEY.param(),
                                          HCPCSOptions.SECRET_KEY.value());
            logParameter(HCPCSOptions.SECRET_KEY.param(),
                         (StringUtils.isNullOrEmpty(secretKey) ? "is unset" : "is set"));

            String proxyHost = config.get(HCPCSOptions.PROXY_HOST.param(),
                                          HCPCSOptions.PROXY_HOST.value());
            logParameter(HCPCSOptions.PROXY_HOST.param(), proxyHost);

            String proxyPort = config.get(HCPCSOptions.PROXY_PORT.param(),
                                          HCPCSOptions.PROXY_PORT.value());
            logParameter(HCPCSOptions.PROXY_PORT.param(), proxyPort);

            String connectionTTL = config.get(HCPCSOptions.CONNECTION_TTL.param(),
                                              HCPCSOptions.CONNECTION_TTL.value());
            logParameter(HCPCSOptions.CONNECTION_TTL.param(), connectionTTL);

            String connectionTimeout = config.get(HCPCSOptions.CONNECTION_TIMEOUT.param(),
                                                  HCPCSOptions.CONNECTION_TIMEOUT.value());
            logParameter(HCPCSOptions.CONNECTION_TIMEOUT.param(), connectionTimeout);

            String socketTimeout = config.get(HCPCSOptions.SOCKET_TIMEOUT.param(),
                                              HCPCSOptions.SOCKET_TIMEOUT.value());
            logParameter(HCPCSOptions.SOCKET_TIMEOUT.param(), socketTimeout);

            String requestTimeout = config.get(HCPCSOptions.REQUEST_TIMEOUT.param(),
                                               HCPCSOptions.REQUEST_TIMEOUT.value());
            logParameter(HCPCSOptions.REQUEST_TIMEOUT.param(), requestTimeout);

            try {
                URL endpointUrl = new URL(endpoint);
                String systemName = endpointUrl.getHost();
                String marathonUrl = config.get(HCPCSOptions.MARATHON_URL.param(),
                                                HCPCSOptions.MARATHON_URL.value());
                if (StringUtils.isNullOrEmpty(marathonUrl)) {
                    marathonUrl = String.format("%s://%s:%s", MARATHON_DEFAULT_SCHEME, systemName,
                                                MARATHON_DEFAULT_PORT);
                }
                logParameter(HCPCSOptions.MARATHON_URL.param(), marathonUrl);

                if (StringUtils.isNullOrEmpty(accessKey) || StringUtils.isNullOrEmpty(secretKey)) {
                    throw new StorageException(
                            "accessKey and secretKey must be set to a non-empty value");
                }
                AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(accessKey, secretKey));

                ClientConfiguration clientCfg = new ClientConfiguration()
                        .withUseExpectContinue(false)
                        .withMaxConnections(maxConnections)
                        .withMaxErrorRetry(maxErrorRetry);

                if (!StringUtils.isNullOrEmpty(resolver)) {
                    DnsResolver dnsResolver = getResolver(resolver, systemName, marathonUrl);
                    clientCfg.withDnsResolver(dnsResolver);
                }

                if (!StringUtils.isNullOrEmpty(proxyHost)
                        && !StringUtils.isNullOrEmpty(proxyPort)) {
                    clientCfg.withProxyHost(proxyHost).withProxyPort(Integer.valueOf(proxyPort));
                }

                if (!StringUtils.isNullOrEmpty(connectionTTL)) {
                    clientCfg.withConnectionTTL(Long.valueOf(connectionTTL));
                }

                if (!StringUtils.isNullOrEmpty(connectionTimeout)) {
                    clientCfg.withConnectionTimeout(Integer.valueOf(connectionTimeout));
                }

                if (!StringUtils.isNullOrEmpty(socketTimeout)) {
                    clientCfg.withSocketTimeout(Integer.valueOf(socketTimeout));
                }

                if (!StringUtils.isNullOrEmpty(requestTimeout)) {
                    clientCfg.withRequestTimeout(Integer.valueOf(requestTimeout));
                }

                EndpointConfiguration endpointCfg = new EndpointConfiguration(endpoint, region);
                s3Client = AmazonS3ClientBuilder.standard()
                        .withCredentials(credentialsProvider)
                        .withClientConfiguration(clientCfg)
                        .withEndpointConfiguration(endpointCfg)
                        .withPathStyleAccessEnabled(pathStyleAccess)
                        .build();
            } catch (Throwable e) {
                logger.error("Exception during init", e);
                throw new StorageException(e);
            }
        }
    }

    private DnsResolver getResolver(String type, String systemName, String marathonUrl)
            throws UnknownHostException, MalformedURLException {
        String resolver = type.toLowerCase();
        if ("static".equals(resolver)) {
            return new StaticRRDnsResolver(systemName);
        }
        if ("dynamic".equals(resolver)) {
            return new DynamicRRDnsResolver(systemName, RESOLVER_PERIOD, RESOLVER_UNIT, logger);
        }
        if ("marathon".equals(resolver)) {
            return new DynamicRRMarathonResolver(marathonUrl, HCPCS_S3_APP_ID, RESOLVER_PERIOD,
                    RESOLVER_UNIT, logger);
        }
        throw new StorageException("Unknown resolver type: " + type);
    }

    private void logParameter(String name, String value) {
        logger.info("HCPCS parameter: {} = {}", name, (value == null) ? "null" : value);
    }

    @Override
    public void dispose() {
        synchronized (this) {
            super.dispose();
            if (s3Client != null) {
                s3Client.shutdown();
                s3Client = null;
            }
        }
    }

    @Override
    public void createContainer(String container, Config config) {
        super.createContainer(container, config);
        try {
            s3Client.createBucket(container);
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
                return;
            }
            throw new StorageException(e);
        } catch (SdkClientException e) {
            throw new StorageException(e);
        }

    }

    @Override
    public void deleteContainer(String container, Config config) {
        super.deleteContainer(container, config);
        try {
            s3Client.deleteBucket(container);
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                return;
            }
            throw new StorageException(e);
        } catch (SdkClientException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createObject(String container, String object, InputStream data,
                             long length, Config config) {
        super.createObject(container, object, data, length, config);
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(length);
            metadata.setContentType("application/octet-stream");
            PutObjectRequest request = new PutObjectRequest(container, object, data, metadata);
            s3Client.putObject(request);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public InputStream getObject(String container, String object, Config config) {
        super.getObject(container, object, config);
        try {
            GetObjectRequest request = new GetObjectRequest(container, object);
            S3Object response = s3Client.getObject(request);
            return (response != null) ? response.getObjectContent() : null;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteObject(String container, String object, Config config) {
        super.deleteObject(container, object, config);
        try {
            DeleteObjectRequest request = new DeleteObjectRequest(container, object);
            s3Client.deleteObject(request);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

}
