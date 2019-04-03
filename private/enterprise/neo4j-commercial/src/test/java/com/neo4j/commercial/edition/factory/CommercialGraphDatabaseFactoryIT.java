/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition.factory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.io.fs.FileSystemUtils.isEmptyOrNonExistingDirectory;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class CommercialGraphDatabaseFactoryIT
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void configuredDatabasesRootPath()
    {
        File factoryDir = testDirectory.databaseDir();
        File databasesDir = testDirectory.directory( "my_databases" );

        GraphDatabaseService db = new CommercialGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( factoryDir )
                .setConfig( databases_root_path, databasesDir.toString() )
                .setConfig( online_backup_enabled, FALSE )
                .newGraphDatabase();
        try
        {
            assertTrue( isEmptyOrNonExistingDirectory( fs, new File( factoryDir.getParent(), DEFAULT_DATABASE_NAME ) ) );
            assertTrue( isEmptyOrNonExistingDirectory( fs, new File( factoryDir.getParent(), SYSTEM_DATABASE_NAME ) ) );

            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( databasesDir, DEFAULT_DATABASE_NAME ) ) );
            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( databasesDir, SYSTEM_DATABASE_NAME ) ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    void notConfiguredDatabasesRootPath()
    {
        File factoryDir = testDirectory.databaseDir();

        GraphDatabaseService db = new CommercialGraphDatabaseFactory().newEmbeddedDatabase( factoryDir );
        try
        {
            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( factoryDir.getParent(), DEFAULT_DATABASE_NAME ) ) );
            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( factoryDir.getParent(), SYSTEM_DATABASE_NAME ) ) );
        }
        finally
        {
            db.shutdown();
        }
    }
}
