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
import com.amazonaws.util.StringUtils;
import com.intel.cosbench.log.Logger;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.MarathonClient;
import mesosphere.marathon.client.model.v2.GetTasksResponse;
import mesosphere.marathon.client.model.v2.Task;

public class DynamicRRMarathonResolver implements DnsResolver {
    private List<InetAddress> lookupResult;
    private List<InetAddress> ips;
    private Map<InetAddress, UrlTester> testers;
    private final String host;
    private final String appName;
    private final Marathon marathon;
    private final ScheduledThreadPoolExecutor executor;
    private int i = 0;
    private final Logger log;

    /**
     * Resolves clusterName to ips where appName (i.e. clientaccess.data) is running (per marathon).
     * Any other host will go through the default resolver. This class will query marathon for
     * updates at interval specified by period and unit parameters.
     * <p>
     * resolve() returns 1 ip address each invocation in a round robin fashion. This is thread safe.
     * All other host lookups go through the default DNS resolver. Thread Safe.
     * </p>
     *
     * @param endpoint marathon endpoint
     * @param appName id (or part of the id) of the application (i.e. metadata.gateway - see
     *            Matathon API)
     * @param period the period between successive executions
     * @param unit the time unit of the period parameters
     * @throws UnknownHostException if unable to resolve the host name
     * @throws MalformedURLException if marathon endpoint is an invalid URL
     */
    public DynamicRRMarathonResolver(String endpoint, String appName, long period,
            TimeUnit unit, Logger logger) throws UnknownHostException, MalformedURLException {
        URL endpointURL = new URL(endpoint);
        host = endpointURL.getHost();
        this.appName = appName;
        log = logger;

        marathon = MarathonClient.getInstance(endpoint);
        lookupResult = new ArrayList<InetAddress>();
        ips = new ArrayList<InetAddress>();
        testers = new HashMap<InetAddress, UrlTester>();

        // execute the first one here to prevent a race and die early
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

    private void performLookup() throws UnknownHostException, MalformedURLException {
        lookupResult.clear();
        GetTasksResponse tasks = marathon.getTasks();
        for (Task task : tasks.getTasks()) {
            String id = task.getAppId();
            if (!StringUtils.isNullOrEmpty(id) && id.contains(appName)) {
                log.debug("Found task:{} host:{}", id, task.getHost());
                InetAddress address = InetAddress.getByName(task.getHost());
                UrlTester tester = tester(address);
                if (tester.isUp()) {
                    lookupResult.add(address);
                } else {
                    log.warn("{} is not up", tester);
                }
            }
        }
        if (lookupResult.isEmpty()) {
            throw new UnknownHostException("No reachable addresses found for " + appName);
        }
        synchronized (host) {
            ips.clear();
            ips.addAll(lookupResult);
        }
    }

    private UrlTester tester(InetAddress address) throws MalformedURLException {
        UrlTester tester = testers.get(address);
        if (tester == null) {
            tester = new UrlTester(new URL("http", address.getHostAddress(), ""));
            testers.put(address, tester);
        }
        return tester;
    }

    @Override
    public InetAddress[] resolve(String arg0) throws UnknownHostException {
        if (this.host.equals(host)) {
            synchronized (host) {
                if (ips.isEmpty()) {
                    throw new UnknownHostException("No addresses for " + appName);
                }
                InetAddress a = ips.get(++i % ips.size());
                return new InetAddress[] { a };
            }
        } else {
            return InetAddress.getAllByName(host);
        }
    }

}
