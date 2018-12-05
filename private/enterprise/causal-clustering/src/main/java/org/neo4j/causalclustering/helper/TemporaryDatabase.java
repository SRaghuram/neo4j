/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import com.neo4j.graphdb.factory.ExternallyManagedPageCache;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.kernel.configuration.Settings.FALSE;

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

        public TemporaryDatabase startTemporaryDatabase( File tempStore, String txLogsDirectoryName, String recordFormat )
        {
            ExternallyManagedPageCache.GraphDatabaseFactoryWithPageCacheFactory factory =
                    ExternallyManagedPageCache.graphDatabaseFactoryWithPageCache( pageCache );

            GraphDatabaseService db = factory
                    .setUserLogProvider( NullLogProvider.getInstance() )
                    .newEmbeddedDatabaseBuilder( tempStore.getAbsoluteFile() )
                    .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                    .setConfig( GraphDatabaseSettings.logical_logs_location, txLogsDirectoryName )
                    .setConfig( GraphDatabaseSettings.active_database, tempStore.getName() )
                    .setConfig( GraphDatabaseSettings.pagecache_warmup_enabled, FALSE )
                    .setConfig( OnlineBackupSettings.online_backup_enabled, FALSE )
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
