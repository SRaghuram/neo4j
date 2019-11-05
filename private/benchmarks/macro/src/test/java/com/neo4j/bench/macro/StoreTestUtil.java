/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.Schema;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.common.util.TestSupport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class StoreTestUtil
{
    // Create empty store with valid schema, as expected by workload
    public static Store createEmptyStoreFor( Workload workload, Path storePath, Path neo4jConfigFile )
    {
        try
        {
            Schema schema = workload.expectedSchema();
            Store store = TestSupport.createEmptyStore( Files.createTempDirectory( storePath, "store" ), neo4jConfigFile );
            EmbeddedDatabase.recreateSchema( store, Edition.ENTERPRISE, neo4jConfigFile, schema );
            return store;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
