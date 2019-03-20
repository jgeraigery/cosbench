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

import com.intel.cosbench.api.storage.StorageAPI;
import com.intel.cosbench.api.storage.StorageAPIFactory;

public class HCPCSStorageApiFactory implements StorageAPIFactory {

    private HCPCSStorageApi storageApi = null;

    @Override
    public String getStorageName() {
        return "hcpcs";
    }

    @Override
    public StorageAPI getStorageAPI() {
        // avoid creating a separate client for every worker
        // AWS S3 client is expected to be shared
        synchronized (this) {
            if (storageApi == null) {
                storageApi = new HCPCSStorageApi();
            }
        }
        return storageApi;
    }

}
