/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.com.storecopy.ExternallyManagedPageCache;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.ignore_store_lock;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;

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
                    .setConfig( augmentParams( params ) )
                    .newGraphDatabase();

            return new TemporaryDatabase( db );
        }

        private Map<String,String> augmentParams( Map<String,String> params )
        {
            Map<String,String> augmentedParams = new HashMap<>( params );

            /* This adhoc quiescing of services is unfortunate and fragile, but there really aren't any better options currently. */
            augmentedParams.putIfAbsent( GraphDatabaseSettings.pagecache_warmup_enabled.name(), FALSE );
            augmentedParams.putIfAbsent( OnlineBackupSettings.online_backup_enabled.name(), FALSE );

            /* Touching the store is allowed during bootstrapping. */
            augmentedParams.putIfAbsent( ignore_store_lock.name(), TRUE );

            return augmentedParams;
        }
    }

    @Override
    public void close()
    {
        graphDatabaseService.shutdown();
    }
}
