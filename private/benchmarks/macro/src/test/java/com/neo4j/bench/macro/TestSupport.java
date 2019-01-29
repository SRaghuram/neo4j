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
