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
import com.intel.cosbench.log.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class DynamicRRDnsResolver implements DnsResolver {

    private InetAddress[] ips;
    private final String host;
    private final ScheduledThreadPoolExecutor executor;
    private final Logger log;
    private int i = 0;

    /**
     * Resolves host to multiple IPs using the standard DNS resolver. This class will query DNS at
     * interval specified by period and unit parameters.
     * <p>
     * resolve() returns 1 ip address each invocation in a round robin fashion. All other host
     * lookups go through the default DNS resolver.
     *
     * Thread Safe.
     * </p>
     *
     *
     * @param host host name
     * @param period the period between successive executions
     * @param unit the time unit of the period parameters
     * @throws UnknownHostException if unable to resolve the host name
     */
    public DynamicRRDnsResolver(String host, long period, TimeUnit unit, Logger logger)
            throws UnknownHostException {
        this.host = host;
        log = logger;

        // do this first to fail early and prevent a race
        performLookup();

        executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                return t;
            }
        });
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    performLookup();
                } catch (Throwable e) {
                    log.error("Lookup failed: {}", e.getMessage(), e);
                }
            }
        }, period, period, unit);
    }

    private void performLookup() throws UnknownHostException {
        InetAddress[] lookupResult = InetAddress.getAllByName(host);
        synchronized (host) {
            ips = lookupResult;
        }
    }

    @Override
    /**
     * Returns one IP from the list of known IPs or UnknownHostException() if unable to resolve.
     *
     * @param host host name to resolve
     * @returns single InetAddress in array
     * @throws UnknownHostException if lookups for failed
     */
    public InetAddress[] resolve(String host) throws UnknownHostException {
        if (this.host.equals(host)) {
            if (ips == null) {
                log.warn("No IPs resolved for {}", host);
                throw new UnknownHostException(host);
            }
            synchronized (host) {
                InetAddress a = ips[++i % ips.length];
                return new InetAddress[] { a };
            }
        } else {
            return InetAddress.getAllByName(host);
        }
    }
}
