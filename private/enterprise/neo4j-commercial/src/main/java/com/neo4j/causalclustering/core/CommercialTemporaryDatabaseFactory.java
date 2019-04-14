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

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.io.pagecache.ExternallyManagedPageCache;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.configuration.Settings.FALSE;

public class CommercialTemporaryDatabaseFactory implements TemporaryDatabaseFactory
{
    private final PageCache pageCache;

    public CommercialTemporaryDatabaseFactory( PageCache pageCache )
    {
        this.pageCache = pageCache;
    }

    @Override
    public TemporaryDatabase startTemporaryDatabase( File rootDirectory, Config originalConfig )
    {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( new ExternallyManagedPageCache( pageCache ) );
        DatabaseManagementService managementService = new CommercialGraphDatabaseFactory()
                .setUserLogProvider( NullLogProvider.getInstance() )
                .setExternalDependencies( dependencies )
                .newEmbeddedDatabaseBuilder( rootDirectory )
                .setConfig( augmentConfig( originalConfig, rootDirectory ) ).newDatabaseManagementService();
        return new TemporaryDatabase( managementService );
    }

    private static Map<String,String> augmentConfig( Config originalConfig, File rootDirectory )
    {
        Map<String,String> augmentedParams = new HashMap<>();

        // use the same record format as specified by the original config
        augmentedParams.put( GraphDatabaseSettings.record_format.name(), originalConfig.get( GraphDatabaseSettings.record_format ) );

        // make all database and transaction log directories live in the specified root directory
        augmentedParams.put( GraphDatabaseSettings.databases_root_path.name(), rootDirectory.getAbsolutePath() );
        augmentedParams.put( GraphDatabaseSettings.transaction_logs_root_path.name(), rootDirectory.getAbsolutePath() );

        /* This adhoc quiescing of services is unfortunate and fragile, but there really aren't any better options currently. */
        augmentedParams.putIfAbsent( GraphDatabaseSettings.pagecache_warmup_enabled.name(), FALSE );
        augmentedParams.putIfAbsent( OnlineBackupSettings.online_backup_enabled.name(), FALSE );
        augmentedParams.putIfAbsent( "metrics.enabled", Settings.FALSE );
        augmentedParams.putIfAbsent( "metrics.csv.enabled", Settings.FALSE );

        return augmentedParams;
    }
}
