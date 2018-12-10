/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import com.neo4j.graphdb.factory.ExternallyManagedPageCache;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.NullLogProvider;

public class TemporaryDatabase implements AutoCloseable
{
    private final GraphDatabaseService graphDatabaseService;

    private TemporaryDatabase( GraphDatabaseService graphDatabaseService )
    {
        this.graphDatabaseService = graphDatabaseService;
    }

    public GraphDatabaseService graphDatabaseService()
    {
        return graphDatabaseService;
    }

    public static class Factory
    {
        private final PageCache pageCache;

        public Factory( PageCache pageCache )
        {
            this.pageCache = pageCache;
        }

        public TemporaryDatabase startTemporaryDatabase( File databaseDirectory, Map<String,String> params ) throws IOException
        {
            ExternallyManagedPageCache.GraphDatabaseFactoryWithPageCacheFactory factory =
                    ExternallyManagedPageCache.graphDatabaseFactoryWithPageCache( pageCache );

            GraphDatabaseService db = factory
                    .setUserLogProvider( NullLogProvider.getInstance() )
                    .newEmbeddedDatabaseBuilder( databaseDirectory )
                    .setConfig( params )
                    .newGraphDatabase();

            return new TemporaryDatabase( db );
        }
    }

    @Override
    public void close()
    {
        graphDatabaseService.shutdown();
    }
}

