/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.Schema;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.options.Edition;
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
            // Ensure defaults are set
            Neo4jConfigBuilder.withDefaults()
                              .mergeWith( Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build() )
                              .writeToFile( neo4jConfigFile );

            Store store = TestSupport.createEmptyStore( Files.createTempDirectory( storePath, "store" ), neo4jConfigFile );
            recreateSchema( store, neo4jConfigFile, workload );
            return store;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public static Store createTemporaryEmptyStoreFor( Workload workload, Path storePath, Path neo4jConfigFile )
    {
        Store store = TestSupport.createTemporaryEmptyStore( storePath, neo4jConfigFile );
        recreateSchema( store, neo4jConfigFile, workload );
        return store;
    }

    private static void recreateSchema( Store store, Path neo4jConfigFile, Workload workload )
    {
        Schema schema = workload.expectedSchema();
        EmbeddedDatabase.recreateSchema( store, Edition.ENTERPRISE, neo4jConfigFile, schema );
    }
}
