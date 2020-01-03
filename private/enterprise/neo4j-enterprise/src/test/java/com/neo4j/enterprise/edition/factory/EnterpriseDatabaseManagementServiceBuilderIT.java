/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.edition.factory;

import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
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
        File factoryDir = testDirectory.homeDir();
        File databasesDir = testDirectory.directory( "my_databases" );

        DatabaseManagementService managementService = new EnterpriseDatabaseManagementServiceBuilder( factoryDir )
                .setConfig( databases_root_path, databasesDir.toPath() )
                .setConfig( online_backup_enabled, false )
                .build();
        try
        {
            assertTrue( isEmptyOrNonExistingDirectory( fs, new File( factoryDir, DEFAULT_DATABASE_NAME ) ) );
            assertTrue( isEmptyOrNonExistingDirectory( fs, new File( factoryDir, SYSTEM_DATABASE_NAME ) ) );

            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( databasesDir, DEFAULT_DATABASE_NAME ) ) );
            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( databasesDir, SYSTEM_DATABASE_NAME ) ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void notConfiguredDatabasesRootPath()
    {
        File factoryDir = testDirectory.homeDir();
        File storeDir = neo4jLayout.databasesDirectory();

        DatabaseManagementService managementService = new EnterpriseDatabaseManagementServiceBuilder( factoryDir ).build();
        try
        {
            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( storeDir, DEFAULT_DATABASE_NAME ) ) );
            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( storeDir, SYSTEM_DATABASE_NAME ) ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
