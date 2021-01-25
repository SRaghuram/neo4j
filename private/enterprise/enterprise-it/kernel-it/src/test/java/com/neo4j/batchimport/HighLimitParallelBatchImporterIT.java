/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.batchimport;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import org.neo4j.internal.batchimport.ParallelBatchImporter;
import org.neo4j.internal.batchimport.ParallelBatchImporterTest;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * Test for {@link ParallelBatchImporter} in an enterprise environment so that enterprise store is used.
 */
public class HighLimitParallelBatchImporterIT extends ParallelBatchImporterTest
{
    @Override
    public RecordFormats getFormat()
    {
        return HighLimit.RECORD_FORMATS;
    }

    @Override
    public TestDatabaseManagementServiceBuilder getDBMSBuilder( DatabaseLayout layout )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( layout );
    }
}
