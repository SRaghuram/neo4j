/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnterpriseDbmsExtension
class EnterpriseDbmsExtensionInjectionTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseManagementService dbms;
    @Inject
    private GraphDatabaseService db;
    @Inject
    private GraphDatabaseAPI dbApi;
    @Inject
    private DbmsController dbmsController;
    @Inject
    private Neo4jLayout neo4jLayout;
    @Inject
    private DatabaseLayout databaseLayout;

    @RepeatedTest( 2 )
    void shouldInject()
    {
        assertAllIsInjected();
    }

    @ParameterizedTest
    @ValueSource( ints = {5, 6} )
    void shouldInject( int a )
    {
        assertTrue( a > 0 );
        assertAllIsInjected();
    }

    @Test
    void shouldRestart()
    {
        // given
        File preHomeDir = testDirectory.homeDir();

        // when
        var databaseName = dbApi.databaseName();
        dbmsController.restartDbms( databaseName );

        assertEquals( databaseName, dbApi.databaseName() );
        assertTrue( dbApi.isAvailable( 0 ) );

        // then
        File postHomeDir = testDirectory.homeDir();
        assertEquals( preHomeDir, postHomeDir );
        assertAllIsInjected();
    }

    private void assertAllIsInjected()
    {
        assertNotNull( fs );
        assertNotNull( testDirectory );
        assertNotNull( dbms );
        assertNotNull( db );
        assertNotNull( dbApi );
        assertNotNull( neo4jLayout );
        assertNotNull( databaseLayout );

        assertEquals( testDirectory.getFileSystem(), fs );
        assertTrue( fs instanceof DefaultFileSystemAbstraction );

        assertSame( db, dbApi );
        assertEquals( testDirectory.homePath(), neo4jLayout.homeDirectory() );
    }
}
