/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.client.database.Store;

import java.nio.file.Path;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class TestSupport
{
    public static Store createEmptyStore( Path storeDir )
    {
        Store store = Store.createEmptyAt( storeDir );
        new GraphDatabaseFactory().newEmbeddedDatabase( store.graphDbDirectory().toFile() ).shutdown();
        return store;
    }
}
