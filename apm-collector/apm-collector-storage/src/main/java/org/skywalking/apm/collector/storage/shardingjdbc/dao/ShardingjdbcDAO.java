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

import org.skywalking.apm.collector.client.h2.H2Client;
import org.skywalking.apm.collector.client.h2.H2ClientException;
import org.skywalking.apm.collector.client.shardingjdbc.ShardingjdbcClient;
import org.skywalking.apm.collector.client.shardingjdbc.ShardingjdbcClientException;
import org.skywalking.apm.collector.storage.dao.DAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author wangkai
 */
public abstract class ShardingjdbcDAO extends DAO<ShardingjdbcClient> {
    private final Logger logger = LoggerFactory.getLogger(ShardingjdbcDAO.class);

    protected final int getMaxId(String tableName, String columnName) {
        String sql = "select max(" + columnName + ") from " + tableName;
        return getIntValueBySQL(sql);
    }

    protected final int getMinId(String tableName, String columnName) {
        String sql = "select min(" + columnName + ") from " + tableName;
        return getIntValueBySQL(sql);
    }

    private int getIntValueBySQL(String sql) {
        ShardingjdbcClient client = getClient();
        try (ResultSet rs = client.executeQuery(sql, null)) {
            if (rs.next()) {
                int id = rs.getInt(1);
                if (id == Integer.MAX_VALUE || id == Integer.MIN_VALUE) {
                    return 0;
                } else {
                    return id;
                }
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return 0;
    }
}
