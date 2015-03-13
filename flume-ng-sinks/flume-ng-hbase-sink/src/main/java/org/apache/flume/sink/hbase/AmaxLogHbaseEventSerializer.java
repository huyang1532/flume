/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.hbase;

import com.google.common.collect.Lists;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Created by young on 2014/12/24.
 */
public class AmaxLogHbaseEventSerializer implements HbaseEventSerializer {


    public  static final Logger logger = LoggerFactory.getLogger(AmaxLogHbaseEventSerializer.class);
    /** Comma separated list of column names to place matching groups in. */
    public static final String COL_NAME_CONFIG = "colNames";
    private byte[] cf;
    private List<byte[]> payload = Lists.newArrayList();
    private List<byte[]> colNames = Lists.newArrayList();
    private Charset charset = Charset.defaultCharset();
    private static final Character sparcers = (char) 0x09;

    @Override
    public void initialize(Event event, byte[] columnFamily) {

        this.cf = columnFamily;
        byte[] body = event.getBody();
        String bodyStr = new String(body, 0, body.length, charset);
        String[] values = bodyStr.split(String.valueOf(sparcers));
        logger.debug("bodyStr is " + bodyStr);
        this.payload.clear();
        for (String value : values) {

            this.payload.add(value.getBytes(charset));
        }
        logger.debug("payload size is " + this.payload.size());
    }

    @Override
    public List<Row> getActions() {

        List<Row> actions = Lists.newArrayList();
        String rowKeyStr = new String(payload.get(1)) + new String(payload.get(0));
        logger.debug("rowkey str is " + rowKeyStr);
        byte[] rowKey =  rowKeyStr.getBytes(); //bid作为rowkey；
        Put put = new Put(rowKey);
//        put.add(cf, colNames.get(0), payload.get(0));
//        logger.info("colNames size is " + colNames.size() + ", payload size is " + payload.size());
        if (colNames.size() > payload.size()) {
            
            logger.warn("data length is bigger than colNames length");
        } else {
            for (int i = 2; i < payload.size(); i++) {
                put.add(cf, colNames.get(i), payload.get(i));
            }
        }
        actions.add(put);
        return actions;
    }

    @Override
    public List<Increment> getIncrements() {
        return Lists.newArrayList();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Context context) {

        String colNameStr = context.getString(COL_NAME_CONFIG);
        String[] columnNames = colNameStr.replaceAll(" ", "").split(",");
        logger.debug("colNameStr is " + colNameStr);
        for (String s : columnNames) {
            colNames.add(s.getBytes(charset));
        }

    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }
}
