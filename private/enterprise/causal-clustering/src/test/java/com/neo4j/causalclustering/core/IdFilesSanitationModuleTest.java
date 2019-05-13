/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class IdFilesSanitationModuleTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldDeleteIdFilesForTheSpecifiedDatabase() throws Exception
    {
        var databaseIdRepository = new TestDatabaseIdRepository();
        var databaseId1 = databaseIdRepository.get( "database1" );
        var databaseId2 = databaseIdRepository.get( "database2" );
        var databaseLayout1 = testDirectory.databaseLayout( databaseId1.name() );
        var databaseLayout2 = testDirectory.databaseLayout( databaseId2.name() );
        createIdFiles( databaseLayout1 );
        createIdFiles( databaseLayout2 );

        var coreStateCheck = mock( StartupCoreStateCheck.class );
        when( coreStateCheck.wasUnboundOnStartup() ).thenReturn( true );

        var databaseManager = new StubClusteredDatabaseManager();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId1 ).withDatabaseLayout( databaseLayout1 ).register();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId2 ).withDatabaseLayout( databaseLayout2 ).register();

        var idFilesSanitationModule = new IdFilesSanitationModule( coreStateCheck, databaseId1, databaseManager, fs, NullLogProvider.getInstance() );

        idFilesSanitationModule.start();

        assertAllIdFilesMissing( databaseLayout1 );
        assertAllIdFilesExist( databaseLayout2 );
    }

    private void createIdFiles( DatabaseLayout databaseLayout ) throws IOException
    {
        for ( File idFile : databaseLayout.idFiles() )
        {
            fs.write( idFile ).close();
        }
    }

    private void assertAllIdFilesMissing( DatabaseLayout databaseLayout )
    {
        assertAll( databaseLayout.idFiles()
                .stream()
                .map( idFile -> () -> assertFalse( fs.fileExists( idFile ), idFile + " exists" ) ) );
    }

    private void assertAllIdFilesExist( DatabaseLayout databaseLayout )
    {
        assertAll( databaseLayout.idFiles()
                .stream()
                .map( idFile -> () -> assertTrue( fs.fileExists( idFile ), idFile + " does not exist" ) ) );
    }
}
