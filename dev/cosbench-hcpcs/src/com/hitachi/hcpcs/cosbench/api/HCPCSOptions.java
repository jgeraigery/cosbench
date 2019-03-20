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
 * or implied.
 *
 * See the License for the specific language governing permissions and limitations under the
 * License.
 *
 * ========================================================================
 */
package com.hitachi.hcpcs.cosbench.api;

public enum HCPCSOptions {
    MARATHON_URL("marathonUrl", null),
    ACCESS_KEY("accessKey", null),
    SECRET_KEY("secretKey", null),
    ENDPOINT("endpoint", null),
    PROXY_HOST("proxyHost", null),
    PROXY_PORT("proxyPort", null),
    CONNECTION_TTL("connTTL", null),
    CONNECTION_TIMEOUT("connTimeout", null),
    SOCKET_TIMEOUT("sockTimeout", null),
    REQUEST_TIMEOUT("reqTimeout", null),
    PATH_STYLE_ACCESS("pathStyleAccess", "true"),
    REGION("region", "us-east-1"),
    RESOLVER("resolver", "static"),
    INSECURE("insecure", "true"),
    MD5_DISABLE("md5Disable", "false"),
    MAX_CONNECTIONS("maxConn", "4096"),
    MAX_ERROR_RETRY("maxErrRetry", "0");

    private String param;
    private String value;

    private HCPCSOptions(String param, String value) {
        this.param = param;
        this.value = value;
    }

    public String param() {
        return this.param;
    }

    public String value() {
        return this.value;
    }

}
