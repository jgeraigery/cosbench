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
package com.hitachi.hcpcs.cosbench.resolver;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class UrlTester {
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 100;
    private static final int DEFAULT_READ_TIMEOUT_MS = 500;
    private static final String DEFAULT_HTTP_METHOD = "OPTIONS";
    private static HostnameVerifier trustAllHostnames = new HostnameVerifier() {
        @Override
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    };
    private static TrustManager[] trustAllCerts = new TrustManager[] {
            new X509TrustManager() {
                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }

                @Override
                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }

            } };
    private static boolean initialized = false;
    private final URL url;

    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final String httpMethod;

    /**
     * Create a HTTP or HTTPS url tester with some some defaults. This just performs a request and
     * checks that it got a resposne code. Any response code.
     *
     * <pre>
     * connection timeout = 100ms
     * read timeout       = 500ms
     * http method        = OPTIONS
     * </pre>
     *
     * @param url url to check
     */
    public UrlTester(URL url) {
        this(url, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS, DEFAULT_HTTP_METHOD);
    }

    /**
     * Create a HTTP or HTTPS url tester.
     *
     * @param url url to check
     * @param connectTimeoutMillis connection timeout in milliseconds
     * @param readTimeoutMillis read timeout in milliseconds
     * @param httpMethod http method to use in request
     */
    public UrlTester(URL url, int connectTimeoutMillis, int readTimeoutMillis, String httpMethod) {
        if (url == null) {
            throw new IllegalArgumentException("url cannot be null");
        }
        if (connectTimeoutMillis <= 0) {
            throw new IllegalArgumentException("connectionTimeoutMillis must be > 0");
        }
        if (readTimeoutMillis <= 0) {
            throw new IllegalArgumentException("readTimeoutMillis must be > 0");
        }
        if (httpMethod == null) {
            throw new IllegalArgumentException("httpMethod cannot be null");
        }
        this.url = url;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
        this.httpMethod = httpMethod;
        initSSL();
    }

    private synchronized void initSSL() {
        if (initialized) {
            return;
        }
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (Exception e) {
            // since these values are hard coded and correct, this should not fail
            // if it does, there is probably a larger problem
            throw new RuntimeException("Unexpected SSL initialization failure", e);
        }
        initialized = true;
    }

    /**
     * Checks if url is reachable.
     *
     * @return true if reachable, false otherwise.
     */
    public boolean isUp() {
        try {
            return check();
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Check if url is reachable.
     *
     * @return true if reachable, false otherwise.
     * @throws IOException why the call failed
     */
    public boolean check() throws IOException {
        HttpURLConnection con = null;
        try {
            con = (HttpURLConnection) url.openConnection();
            if (con instanceof HttpsURLConnection) {
                ((HttpsURLConnection) con).setHostnameVerifier(trustAllHostnames);
            }
            con.setConnectTimeout(connectTimeoutMillis);
            con.setReadTimeout(readTimeoutMillis);
            con.setRequestMethod(httpMethod);
            int response = con.getResponseCode();
            if (response == -1) {
                throw new IOException("Invalid HTTP Response: -1");
            }
            return true;
        } finally {
            if (con != null) {
                con.disconnect();
            }
        }
    }

    @Override
    public String toString() {
        return url.toString();
    }
}
