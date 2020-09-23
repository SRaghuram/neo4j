/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.helper.TemporaryDatabase;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.SystemGraphDbmsModel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@EphemeralPageCacheExtension
@EphemeralTestDirectoryExtension
public class EnterpriseTemporaryDatabaseFactoryTest
{
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory testDirectory;

    private TemporaryDatabaseFactory temporaryDatabaseFactory;
    private TemporaryDatabase temporaryDatabase;

    @BeforeEach
    void beforeEach()
    {
        temporaryDatabaseFactory = new EnterpriseTemporaryDatabaseFactory( pageCache, testDirectory.getFileSystem() );
    }

    @AfterEach
    void afterEach()
    {
        if ( temporaryDatabase != null )
        {
            temporaryDatabase.close();
            temporaryDatabase = null;
        }
    }

    @Test
    void checkSystemDatabase()
    {
        // given
        var config = Config.defaults();
        var tempRoot = testDirectory.directory( "tempRoot" );

        // when
        temporaryDatabase = temporaryDatabaseFactory.startTemporaryDatabase( tempRoot, config, true );

        // then
        assertEquals( GraphDatabaseSettings.SYSTEM_DATABASE_NAME, temporaryDatabase.graphDatabaseService().databaseName() );
        assertDefaultDatabaseName( temporaryDatabase, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    @Test
    void checkSystemDatabaseWithCustomDefaultDatabase()
    {
        // given
        var config = Config.defaults();
        var tempRoot = testDirectory.directory( "tempRoot" );

        var customDefaultDatabase = "boat";
        config.set( GraphDatabaseSettings.default_database, customDefaultDatabase );

        // when
        temporaryDatabase = temporaryDatabaseFactory.startTemporaryDatabase( tempRoot, config, true );

        // then
        assertEquals( GraphDatabaseSettings.SYSTEM_DATABASE_NAME, temporaryDatabase.graphDatabaseService().databaseName() );
        assertDefaultDatabaseName( temporaryDatabase, customDefaultDatabase );
    }

    @Test
    void checkDefaultDatabase()
    {
        // given
        var config = Config.defaults();
        var tempRoot = testDirectory.directory( "tempRoot" );

        var customDefaultDatabase = "boat";
        config.set( GraphDatabaseSettings.default_database, customDefaultDatabase );

        // when
        temporaryDatabase = temporaryDatabaseFactory.startTemporaryDatabase( tempRoot, config, false );

        // then name is default even if custom name is set
        assertEquals( GraphDatabaseSettings.DEFAULT_DATABASE_NAME, temporaryDatabase.graphDatabaseService().databaseName() );
        assertDatabaseIsEmpty( temporaryDatabase );
    }

    private static void assertDefaultDatabaseName( TemporaryDatabase temporaryDatabase, String customDefaultDatabase )
    {
        try ( var tx = temporaryDatabase.graphDatabaseService().beginTx() )
        {
            var defaultDatabase = tx.findNode( SystemGraphDbmsModel.DATABASE_LABEL, SystemGraphDbmsModel.DATABASE_DEFAULT_PROPERTY, true );
            assertEquals( customDefaultDatabase, defaultDatabase.getProperty( SystemGraphDbmsModel.DATABASE_NAME_PROPERTY ) );
        }
    }

    private static void assertDatabaseIsEmpty( TemporaryDatabase temporaryDatabase )
    {
        try ( var tx = temporaryDatabase.graphDatabaseService().beginTx() )
        {
            assertEquals( 0, tx.getAllNodes().stream().count() );
        }
    }
}
