/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.backup.impl.BackupProtocolService.startTemporaryDb;

class BackupRecoveryService
{
    void recoverWithDatabase( Path databaseDirectory, PageCache pageCache, Config config )
    {
        DatabaseLayout databaseLayout = DatabaseLayout.of( databaseDirectory.toFile() );
        Map<String,String> configParams = config.getRaw();
        configParams.put( GraphDatabaseSettings.logical_logs_location.name(), databaseDirectory.toString() );
        configParams.put( GraphDatabaseSettings.active_database.name(), databaseLayout.getDatabaseName() );
        configParams.put( GraphDatabaseSettings.pagecache_warmup_enabled.name(), Settings.FALSE );
        GraphDatabaseAPI targetDb = startTemporaryDb( databaseLayout.databaseDirectory(), pageCache, configParams );
        targetDb.shutdown();
        // as soon as recovery will be extracted we will not gonna need this
        File lockFile = databaseLayout.getStoreLayout().storeLockFile();
        if ( lockFile.exists() )
        {
            FileUtils.deleteFile( lockFile );
        }
    }
}
