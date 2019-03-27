/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.util;

import com.neo4j.bench.client.database.Store;
import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;

import java.nio.file.Path;

import org.neo4j.configuration.GraphDatabaseSettings;

public class TestSupport
{
    public static Store createEmptyStore( Path storeDir )
    {
        Path graphDbDir = storeDir.resolve( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        BenchmarkUtil.assertDoesNotExist( graphDbDir );
        new CommercialGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( graphDbDir.toFile() )
                .setConfig( GraphDatabaseSettings.transaction_logs_root_path, storeDir.toAbsolutePath().toString() )
                .newGraphDatabase()
                .shutdown();
        return Store.createFrom( storeDir );
    }
}
