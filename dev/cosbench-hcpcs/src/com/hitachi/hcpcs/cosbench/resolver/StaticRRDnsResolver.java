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

import com.amazonaws.DnsResolver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

public class StaticRRDnsResolver implements DnsResolver {

    private final InetAddress[] ips;
    private final AtomicInteger i = new AtomicInteger();
    private final String host;

    /**
     * Resolves host to multiple IPs once and returns those IPs round-robin when resolving the
     * provided host. Any other host will go through the default resolver. Thread Safe.
     *
     * @param host host name
     * @throws UnknownHostException if unable to resolve the host name
     */
    public StaticRRDnsResolver(String host) throws UnknownHostException {
        this.host = host;
        ips = InetAddress.getAllByName(host);
    }

    @Override
    public InetAddress[] resolve(String host) throws UnknownHostException {
        if (this.host.equals(host)) {
            InetAddress a = ips[i.getAndIncrement() % ips.length];
            return new InetAddress[] { a };
        } else {
            return InetAddress.getAllByName(host);
        }
    }

}
