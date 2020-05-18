/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.helper.TemporaryDatabase;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;
import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;

import java.io.File;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.ExternallyManagedPageCache;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.NullLogProvider;

public class EnterpriseTemporaryDatabaseFactory implements TemporaryDatabaseFactory
{
    private final PageCache pageCache;
    private final FileSystemAbstraction fileSystem;

    public EnterpriseTemporaryDatabaseFactory( PageCache pageCache, FileSystemAbstraction fileSystem )
    {
        this.pageCache = pageCache;
        this.fileSystem = fileSystem;
    }

    @Override
    public TemporaryDatabase startTemporaryDatabase( File rootDirectory, Config originalConfig, boolean isSystem )
    {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( new ExternallyManagedPageCache( pageCache ) );
        dependencies.satisfyDependency( fileSystem );
        var managementServiceBuilder = new EnterpriseDatabaseManagementServiceBuilder( rootDirectory )
                .setUserLogProvider( NullLogProvider.getInstance() )
                .setExternalDependencies( dependencies );

        augmentConfig( managementServiceBuilder, originalConfig, rootDirectory );
        return new TemporaryDatabase( managementServiceBuilder.build(), isSystem );
    }

    private static void augmentConfig( DatabaseManagementServiceBuilder managementServiceBuilder, Config originalConfig, File rootDirectory )
    {
        // use the same record format as specified by the original config
        managementServiceBuilder.setConfig( GraphDatabaseSettings.record_format, originalConfig.get( GraphDatabaseSettings.record_format ) );

        // make all database and transaction log directories live in the specified root directory
        managementServiceBuilder.setConfig( GraphDatabaseSettings.databases_root_path, rootDirectory.toPath().toAbsolutePath() );
        managementServiceBuilder.setConfig( GraphDatabaseSettings.transaction_logs_root_path, rootDirectory.toPath().toAbsolutePath() );

        /* This adhoc quiescing of services is unfortunate and fragile, but there really aren't any better options currently. */
        managementServiceBuilder.setConfig( GraphDatabaseSettings.pagecache_warmup_enabled, false );
        managementServiceBuilder.setConfig( OnlineBackupSettings.online_backup_enabled, false );

        managementServiceBuilder.setConfig( MetricsSettings.metricsEnabled, false );
        managementServiceBuilder.setConfig( MetricsSettings.csvEnabled, false );

        // We start the temp database in single instance mode but don't want the cluster-incompatible upgrade to happen
        managementServiceBuilder.setConfig( GraphDatabaseSettings.allow_single_automatic_upgrade, false );
        if ( originalConfig.isExplicitlySet( GraphDatabaseSettings.system_init_file ) )
        {
            // Read custom initialization file from outer dbms
            managementServiceBuilder.setConfig( GraphDatabaseSettings.system_init_file, originalConfig.get( GraphDatabaseSettings.system_init_file ) );
        }
        // Log security initialization to outer dbms log file
        managementServiceBuilder.setConfig( SecuritySettings.security_log_filename, originalConfig.get( SecuritySettings.security_log_filename ) );
        // auth, roles and auth.ini files
        managementServiceBuilder.setConfig( DatabaseManagementSystemSettings.auth_store_directory,
                originalConfig.get( DatabaseManagementSystemSettings.auth_store_directory ) );
        if ( originalConfig.isExplicitlySet( GraphDatabaseSettings.auth_store ) )
        {
            managementServiceBuilder.setConfig( GraphDatabaseSettings.auth_store, originalConfig.get( GraphDatabaseSettings.auth_store ) );
        }
    }
}
