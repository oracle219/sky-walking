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

import org.skywalking.apm.collector.client.h2.H2ClientException;
import org.skywalking.apm.collector.client.shardingjdbc.ShardingjdbcClientException;
import org.skywalking.apm.collector.storage.dao.IBatchDAO;
import org.skywalking.apm.collector.storage.shardingjdbc.define.ShardingjdbcSqlEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wangkai
 */
public class BatchShardingjdbcDAO extends ShardingjdbcDAO implements IBatchDAO {
    private final Logger logger = LoggerFactory.getLogger(BatchShardingjdbcDAO.class);

    @Override
    public void batchPersistence(List<?> batchCollection) {
        if (batchCollection != null && batchCollection.size() > 0) {
            logger.debug("the batch collection size is {}", batchCollection.size());
            Connection conn = null;
            final Map<String, PreparedStatement> batchSqls = new HashMap<>();
            try {
                conn = getClient().getConnection();
                conn.setAutoCommit(true);
                PreparedStatement ps;
                for (Object entity : batchCollection) {
                    ShardingjdbcSqlEntity e = getH2SqlEntity(entity);
                    String sql = e.getSql();
                    if (batchSqls.containsKey(sql)) {
                        ps = batchSqls.get(sql);
                    } else {
                        ps = conn.prepareStatement(sql);
                        batchSqls.put(sql, ps);
                    }

                    Object[] params = e.getParams();
                    if (params != null) {
                        logger.debug("the sql is {}, params size is {}", e.getSql(), params.length);
                        for (int i = 0; i < params.length; i++) {
                            ps.setObject(i + 1, params[i]);
                        }
                    }
                    ps.addBatch();
                }

                for (String k : batchSqls.keySet()) {
                    batchSqls.get(k).executeBatch();
                }
            } catch (SQLException | ShardingjdbcClientException e) {
                logger.error(e.getMessage(), e);
            }
            batchSqls.clear();
        }
    }

    private ShardingjdbcSqlEntity getH2SqlEntity(Object entity) {
        if (entity instanceof ShardingjdbcSqlEntity) {
            return (ShardingjdbcSqlEntity) entity;
        }
        return null;
    }
}
