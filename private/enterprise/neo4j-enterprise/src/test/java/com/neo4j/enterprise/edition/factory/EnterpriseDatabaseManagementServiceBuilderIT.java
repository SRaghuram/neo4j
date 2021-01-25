/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.edition.factory;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.Mode.CORE;
import static org.neo4j.configuration.GraphDatabaseSettings.Mode.READ_REPLICA;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.io.fs.FileSystemUtils.isEmptyOrNonExistingDirectory;

@Neo4jLayoutExtension
class EnterpriseDatabaseManagementServiceBuilderIT
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private Neo4jLayout neo4jLayout;

    @Test
    void configuredDatabasesRootPath()
    {
        Path homeDir = testDirectory.homePath();
        Path databasesDir = testDirectory.directory( "my_databases" );

        DatabaseManagementService managementService = createDbmsBuilder( homeDir )
                .setConfig( databases_root_path, databasesDir )
                .build();
        try
        {
            assertTrue( isEmptyOrNonExistingDirectory( fs, homeDir.resolve( DEFAULT_DATABASE_NAME ) ) );
            assertTrue( isEmptyOrNonExistingDirectory( fs, homeDir.resolve( SYSTEM_DATABASE_NAME ) ) );

            assertFalse( isEmptyOrNonExistingDirectory( fs, databasesDir.resolve( DEFAULT_DATABASE_NAME ) ) );
            assertFalse( isEmptyOrNonExistingDirectory( fs, databasesDir.resolve( SYSTEM_DATABASE_NAME ) ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void notConfiguredDatabasesRootPath()
    {
        Path homeDir = testDirectory.homePath();
        Path storeDir = neo4jLayout.databasesDirectory();

        DatabaseManagementService managementService = createDbmsBuilder( homeDir ).build();
        try
        {
            assertFalse( isEmptyOrNonExistingDirectory( fs, storeDir.resolve( DEFAULT_DATABASE_NAME ) ) );
            assertFalse( isEmptyOrNonExistingDirectory( fs, storeDir.resolve( SYSTEM_DATABASE_NAME ) ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldFailForCore()
    {
        Path homeDir = testDirectory.homePath();

        DatabaseManagementServiceBuilder builder = createDbmsBuilder( homeDir )
                .setConfig( GraphDatabaseSettings.mode, CORE );

        assertThrows( IllegalArgumentException.class, builder::build, "Unsupported mode: CORE" );
    }

    @Test
    void shouldFailForReadReplica()
    {
        Path homeDir = testDirectory.homePath();

        DatabaseManagementServiceBuilder builder = createDbmsBuilder( homeDir )
                .setConfig( GraphDatabaseSettings.mode, READ_REPLICA );

        assertThrows( IllegalArgumentException.class, builder::build, "Unsupported mode: READ_REPLICA" );
    }

    private static DatabaseManagementServiceBuilder createDbmsBuilder( Path homeDirectory )
    {
        //TestEnterpriseDatabaseManagementServiceBuilder is not available in this module
        return new EnterpriseDatabaseManagementServiceBuilder( homeDirectory )
                .setConfig( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( "127.0.0.1",0 ) )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false )
                .setConfig( GraphDatabaseSettings.pagecache_memory, "8m" )
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.kibiBytes( 128 ) )
                .setConfig( BoltConnector.enabled, false );
    }
}
