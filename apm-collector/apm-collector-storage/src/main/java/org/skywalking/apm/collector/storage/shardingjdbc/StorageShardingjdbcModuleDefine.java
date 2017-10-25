/*
 * Copyright 2017, OpenSkywalking Organization All rights reserved.
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
 *
 * Project repository: https://github.com/OpenSkywalking/skywalking
 */

package org.skywalking.apm.collector.storage.shardingjdbc;

import org.skywalking.apm.collector.client.h2.H2Client;
import org.skywalking.apm.collector.client.shardingjdbc.ShardingjdbcClient;
import org.skywalking.apm.collector.core.client.Client;
import org.skywalking.apm.collector.core.framework.DefineException;
import org.skywalking.apm.collector.core.module.ModuleConfigParser;
import org.skywalking.apm.collector.core.storage.StorageInstaller;
import org.skywalking.apm.collector.storage.StorageModuleDefine;
import org.skywalking.apm.collector.storage.StorageModuleGroupDefine;
import org.skywalking.apm.collector.storage.dao.DAOContainer;
import org.skywalking.apm.collector.storage.shardingjdbc.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.shardingjdbc.dao.ShardingjdbcDAODefineLoader;
import org.skywalking.apm.collector.storage.shardingjdbc.define.ShardingjdbcStorageInstaller;

import java.util.List;

/**
 * @author wangkai
 */
public class StorageShardingjdbcModuleDefine extends StorageModuleDefine {

    public static final String MODULE_NAME = "shardingjdbc";

    @Override protected String group() {
        return StorageModuleGroupDefine.GROUP_NAME;
    }

    @Override public String name() {
        return MODULE_NAME;
    }

    @Override public final boolean defaultModule() {
        return true;
    }

    @Override protected ModuleConfigParser configParser() {
        return new StorageShardingjdbcConfigParser();
    }

    @Override protected Client createClient() {
        return new ShardingjdbcClient(StorageShardingjdbcConfig.URL, StorageShardingjdbcConfig.USER_NAME, StorageShardingjdbcConfig.PASSWORD);
    }

    @Override public StorageInstaller storageInstaller() {
        return new ShardingjdbcStorageInstaller();
    }

    @Override public void injectClientIntoDAO(Client client) throws DefineException {
        ShardingjdbcDAODefineLoader loader = new ShardingjdbcDAODefineLoader();
        List<ShardingjdbcDAO> shardingjdbcDAOS = loader.load();
        shardingjdbcDAOS.forEach(shardingjdbcDAO -> {
            shardingjdbcDAO.setClient((ShardingjdbcClient) client);
            String interFaceName = shardingjdbcDAO.getClass().getInterfaces()[0].getName();
            DAOContainer.INSTANCE.put(interFaceName, shardingjdbcDAO);
        });
    }
}
