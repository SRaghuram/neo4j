/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.helper.TemporaryDatabase;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.ignore_store_lock;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class CommercialTemporaryDatabaseFactory implements TemporaryDatabaseFactory
{
    @Override
    public TemporaryDatabase startTemporaryDatabase( PageCache pageCache, File rootDirectory, Map<String,String> params )
    {
        GraphDatabaseService db = new CommercialGraphDatabaseFactory().setUserLogProvider( NullLogProvider.getInstance() )
                .newEmbeddedDatabaseBuilder( rootDirectory )
                .setConfig( augmentParams( params, rootDirectory ) ).newGraphDatabase();

        return new TemporaryDatabase( db );
    }

    private static Map<String,String> augmentParams( Map<String,String> params, File databaseDirectory )
    {
        Map<String,String> augmentedParams = new HashMap<>( params );

        /* This adhoc quiescing of services is unfortunate and fragile, but there really aren't any better options currently. */
        augmentedParams.putIfAbsent( GraphDatabaseSettings.pagecache_warmup_enabled.name(), FALSE );
        augmentedParams.putIfAbsent( OnlineBackupSettings.online_backup_enabled.name(), FALSE );
        augmentedParams.putIfAbsent( "metrics.enabled", Settings.FALSE );
        augmentedParams.putIfAbsent( "metrics.csv.enabled", Settings.FALSE );

        /* Touching the store is allowed during bootstrapping. */
        augmentedParams.putIfAbsent( ignore_store_lock.name(), TRUE );

        augmentedParams.putIfAbsent( GraphDatabaseSettings.store_internal_log_path.name(),
                new File( databaseDirectory, "bootstrap.debug.log" ).getAbsolutePath() );

        return augmentedParams;
    }
}
