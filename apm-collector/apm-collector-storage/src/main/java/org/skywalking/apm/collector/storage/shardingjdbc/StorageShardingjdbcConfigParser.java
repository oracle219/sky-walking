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

import org.skywalking.apm.collector.core.config.ConfigParseException;
import org.skywalking.apm.collector.core.config.SystemConfig;
import org.skywalking.apm.collector.core.module.ModuleConfigParser;
import org.skywalking.apm.collector.core.util.ObjectUtils;
import org.skywalking.apm.collector.core.util.StringUtils;

import java.util.Map;

/**
 * @author wangkai
 */
public class StorageShardingjdbcConfigParser implements ModuleConfigParser {
    private static final String URL = "url";
    public static final String USER_NAME = "user_name";
    public static final String PASSWORD = "password";

    @Override public void parse(Map config) throws ConfigParseException {
        if (ObjectUtils.isNotEmpty(config) && StringUtils.isNotEmpty(config.get(URL))) {
            StorageShardingjdbcConfig.URL = (String)config.get(URL);
        } else {
            StorageShardingjdbcConfig.URL = "jdbc:mysql:" + SystemConfig.DATA_PATH + "/collector?characterEncoding=utf8&useSSL=false";
        }
        if (ObjectUtils.isNotEmpty(config) && StringUtils.isNotEmpty(config.get(USER_NAME))) {
            StorageShardingjdbcConfig.USER_NAME = (String)config.get(USER_NAME);
        } else {
            StorageShardingjdbcConfig.USER_NAME = "root";
        }
        if (ObjectUtils.isNotEmpty(config) && StringUtils.isNotEmpty(config.get(PASSWORD))) {
            StorageShardingjdbcConfig.PASSWORD = (String)config.get(PASSWORD);
        }
    }
}