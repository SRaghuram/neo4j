/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.state.DatabaseFileListing;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestDirectoryExtension
class PrepareStoreCopyFilesTest
{
    @Inject
    public TestDirectory testDirectory;

    private PrepareStoreCopyFiles prepareStoreCopyFiles;
    private DatabaseLayout databaseLayout;
    private DatabaseFileListing.StoreFileListingBuilder fileListingBuilder;

    @BeforeEach
    void setUp()
    {
        var dataSource = mock( Database.class );
        fileListingBuilder = mock( DatabaseFileListing.StoreFileListingBuilder.class, CALLS_REAL_METHODS );
        databaseLayout = DatabaseLayout.ofFlat( testDirectory.directoryPath( "neo4j", "data", "databases" ) );
        when( dataSource.getDatabaseLayout() ).thenReturn( databaseLayout );
        var storeFileListing = mock( DatabaseFileListing.class );
        when( storeFileListing.builder() ).thenReturn( fileListingBuilder );
        when( dataSource.getDatabaseFileListing() ).thenReturn( storeFileListing );
        prepareStoreCopyFiles = new PrepareStoreCopyFiles( dataSource, testDirectory.getFileSystem() );
    }

    @Test
    void shouldHanldeEmptyListOfFilesForeEachType() throws Exception
    {
        setExpectedFiles( new StoreFileMetadata[0] );
        var files = prepareStoreCopyFiles.listReplayableFiles();
        var atomicFilesSnapshot = prepareStoreCopyFiles.getAtomicFilesSnapshot();
        assertEquals( 0, files.length );
        assertEquals( 0, atomicFilesSnapshot.length );
    }

    private void setExpectedFiles( StoreFileMetadata[] expectedFiles ) throws IOException
    {
        doAnswer( invocation -> Iterators.asResourceIterator( Iterators.iterator( expectedFiles ) ) ).when( fileListingBuilder ).build();
    }

    @Test
    void shouldReturnExpectedListOfFileNamesForEachType() throws Exception
    {
        // given
        var expectedFiles = new StoreFileMetadata[]{new StoreFileMetadata( databaseLayout.file( "a" ), 1 ),
                new StoreFileMetadata( databaseLayout.file( "b" ), 2 )};
        setExpectedFiles( expectedFiles );

        // when
        var files = prepareStoreCopyFiles.listReplayableFiles();
        var atomicFilesSnapshot = prepareStoreCopyFiles.getAtomicFilesSnapshot();

        // then
        var expectedFilesConverted = Arrays.stream( expectedFiles ).map( StoreFileMetadata::path ).toArray( Path[]::new );
        var expectedAtomicFilesConverted = Arrays.stream( expectedFiles ).map(
                f -> new StoreResource( f.path(), getRelativePath( f ), f.recordSize(), testDirectory.getFileSystem() ) ).toArray( StoreResource[]::new );
        assertArrayEquals( expectedFilesConverted, files );
        assertEquals( expectedAtomicFilesConverted.length, atomicFilesSnapshot.length );
        for ( var i = 0; i < expectedAtomicFilesConverted.length; i++ )
        {
            StoreResource expected = expectedAtomicFilesConverted[i];
            StoreResource storeResource = atomicFilesSnapshot[i];
            assertEquals( expected.relativePath(), storeResource.relativePath() );
            assertEquals( expected.recordSize(), storeResource.recordSize() );
        }
    }

    private String getRelativePath( StoreFileMetadata f )
    {
        return databaseLayout.databaseDirectory().relativize( f.path() ).toString();
    }
}
