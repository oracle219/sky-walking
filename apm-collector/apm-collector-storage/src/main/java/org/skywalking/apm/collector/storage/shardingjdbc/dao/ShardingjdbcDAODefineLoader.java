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

package org.skywalking.apm.collector.storage.shardingjdbc.dao;

import org.skywalking.apm.collector.core.framework.DefineException;
import org.skywalking.apm.collector.core.framework.Loader;
import org.skywalking.apm.collector.core.util.DefinitionLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangkai
 */
public class ShardingjdbcDAODefineLoader implements Loader<List<ShardingjdbcDAO>> {

    private final Logger logger = LoggerFactory.getLogger(ShardingjdbcDAODefineLoader.class);

    @Override public List<ShardingjdbcDAO> load() throws DefineException {
        List<ShardingjdbcDAO> shardingjdbcDAOS = new ArrayList<>();

        ShardingjdbcDAODefinitionFile definitionFile = new ShardingjdbcDAODefinitionFile();
        logger.info("shardingjdbc dao definition file name: {}", definitionFile.fileName());
        DefinitionLoader<ShardingjdbcDAO> definitionLoader = DefinitionLoader.load(ShardingjdbcDAO.class, definitionFile);
        for (ShardingjdbcDAO dao : definitionLoader) {
            logger.info("loaded shardingjdbc dao definition class: {}", dao.getClass().getName());
            shardingjdbcDAOS.add(dao);
        }
        return shardingjdbcDAOS;
    }
}
