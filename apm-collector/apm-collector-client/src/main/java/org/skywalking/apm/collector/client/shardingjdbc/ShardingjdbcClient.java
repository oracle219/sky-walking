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

package org.skywalking.apm.collector.client.shardingjdbc;

import org.skywalking.apm.collector.core.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author wangkai
 */
public class ShardingjdbcClient implements Client {

    private final Logger logger = LoggerFactory.getLogger(ShardingjdbcClient.class);

    private Connection conn;
    private String url;
    private String userName;
    private String password;

    public ShardingjdbcClient() {
        this.url = "jdbc:mysql://localhost:3306/collector?characterEncoding=utf8&useSSL=false";
        this.userName = "";
        this.password = "";
    }

    public ShardingjdbcClient(String url, String userName, String password) {
        this.url = url;
        this.userName = userName;
        this.password = password;
    }

    @Override public void initialize() throws ShardingjdbcClientException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.
                getConnection(this.url, this.userName, this.password);
        } catch (Exception e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
        }
    }

    @Override public void shutdown() {
        try {
            if(conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(),e);
        }
    }

    public Connection getConnection() throws ShardingjdbcClientException {
        return conn;
    }

    public void execute(String sql) throws ShardingjdbcClientException {
        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            statement.closeOnCompletion();
        } catch (SQLException e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
        }
    }

    public ResultSet executeQuery(String sql, Object[] params) throws ShardingjdbcClientException {
        logger.debug("execute query with result: {}", sql);
        ResultSet rs;
        PreparedStatement statement;
        try {
            statement = getConnection().prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            rs = statement.executeQuery();
            statement.closeOnCompletion();
        } catch (SQLException e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
        }
        return rs;
    }

    public boolean execute(String sql, Object[] params) throws ShardingjdbcClientException {
        logger.debug("execute insert/update/delete: {}", sql);
        boolean flag;
        Connection conn = getConnection();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            conn.setAutoCommit(true);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            flag = statement.execute();
        } catch (SQLException e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
        }
        return flag;
    }
}
