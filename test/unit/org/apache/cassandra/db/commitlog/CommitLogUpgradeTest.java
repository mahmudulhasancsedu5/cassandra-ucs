package org.apache.cassandra.db.commitlog;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;

import junit.framework.Assert;

import com.google.common.base.Predicate;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.Config.CommitFailurePolicy;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.commitlog.CommitLogReplayer.CommitLogReplayException;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.KeyspaceParams;

public class CommitLogUpgradeTest
{
    static final String DATA_DIR = "test/data/legacy-commitlog/";
    static final String PROPERTIES_FILE = "hash.txt";
    static final String CFID_PROPERTY = "cfid";
    static final String CELLS_PROPERTY = "cells";
    static final String HASH_PROPERTY = "hash";

    static final String TABLE = "Standard1";
    static final String KEYSPACE = "Keyspace1";
    static final String CELLNAME = "name";

    @Test
    public void test20() throws Exception
    {
        testRestore(DATA_DIR + "2.0");
    }

    @Test
    public void test21() throws Exception
    {
        testRestore(DATA_DIR + "2.1");
    }

    @Test
    public void test22() throws Exception
    {
        testRestore(DATA_DIR + "2.2");
    }

    @Test
    public void test22_LZ4() throws Exception
    {
        testRestore(DATA_DIR + "2.2-lz4");
    }

    @Test
    public void test22_Snappy() throws Exception
    {
        testRestore(DATA_DIR + "2.2-snappy");
    }

    @Test
    public void test22_truncated() throws Exception
    {
        testRestore(DATA_DIR + "2.2-lz4-truncated");
    }

    @Test(expected = CommitLogReplayException.class)
    public void test22_bitrot() throws Exception
    {
        testRestore(DATA_DIR + "2.2-lz4-bitrot");
    }

    @Test
    public void test22_bitrot_ignored() throws Exception
    {
        try {
            System.setProperty(CommitLogReplayer.IGNORE_REPLAY_ERRORS_PROPERTY, "true");
            testRestore(DATA_DIR + "2.2-lz4-bitrot");
        } finally {
            System.clearProperty(CommitLogReplayer.IGNORE_REPLAY_ERRORS_PROPERTY);
        }
    }

    @Test
    public void test22_bitrot_ignore_policy() throws Exception
    {
        CommitFailurePolicy existingPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try {
            DatabaseDescriptor.setCommitFailurePolicy(CommitFailurePolicy.ignore);
            testRestore(DATA_DIR + "2.2-lz4-bitrot");
        } finally {
            DatabaseDescriptor.setCommitFailurePolicy(existingPolicy);
        }
    }

    @BeforeClass
    static public void initialize() throws FileNotFoundException, IOException, InterruptedException
    {
        CFMetaData metadata = CFMetaData.Builder.createDense(KEYSPACE, TABLE, false, false)
                                                .addPartitionKey("key", AsciiType.instance)
                                                .addClusteringColumn("col", AsciiType.instance)
                                                .addRegularColumn("val", BytesType.instance)
                                                .build()
                                                .compression(SchemaLoader.getCompressionParameters());
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    metadata);
    }

    public void testRestore(String location) throws IOException, InterruptedException
    {
        Properties prop = new Properties();
        prop.load(new FileInputStream(new File(location + File.separatorChar + PROPERTIES_FILE)));
        int hash = Integer.parseInt(prop.getProperty(HASH_PROPERTY));
        int cells = Integer.parseInt(prop.getProperty(CELLS_PROPERTY));

        String cfidString = prop.getProperty(CFID_PROPERTY);
        if (cfidString != null)
        {
            UUID cfid = UUID.fromString(cfidString);
            if (Schema.instance.getCF(cfid) == null)
            {
                CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
                Schema.instance.unload(cfm);
                Schema.instance.load(cfm.copy(cfid));
            }
        }

        Hasher hasher = new Hasher();
        CommitLogTestReplayer replayer = new CommitLogTestReplayer(hasher);
        File[] files = new File(location).listFiles(new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".log");
            }
        });
        replayer.recover(files);

        Assert.assertEquals(cells, hasher.cells);
        Assert.assertEquals(hash, hasher.hash);
    }

    public static int hash(int hash, ByteBuffer bytes)
    {
        int shift = 0;
        for (int i = 0; i < bytes.limit(); i++)
        {
            hash += (bytes.get(i) & 0xFF) << shift;
            shift = (shift + 8) & 0x1F;
        }
        return hash;
    }

    class Hasher implements Predicate<Mutation>
    {
        int hash = 0;
        int cells = 0;

        @Override
        public boolean apply(Mutation mutation)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                for (Row row : update)
                    if (row.clustering().size() > 0 &&
                        AsciiType.instance.compose(row.clustering().get(0)).startsWith(CELLNAME))
                    {
                        for (Cell cell : row.cells())
                        {
                            hash = hash(hash, cell.value());
                            ++cells;
                        }
                    }
            }
            return true;
        }
    }
}
