/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.state.DatabaseFileListing;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileIndexListing;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.io.fs.FileUtils.relativePath;

public class PrepareStoreCopyFilesTest
{
    private final FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemAbstraction );
    private PrepareStoreCopyFiles prepareStoreCopyFiles;
    private NeoStoreFileIndexListing indexListingMock;
    private DatabaseLayout databaseLayout;
    private DatabaseFileListing.StoreFileListingBuilder fileListingBuilder;

    @Before
    public void setUp()
    {
        Database dataSource = mock( Database.class );
        fileListingBuilder = mock( DatabaseFileListing.StoreFileListingBuilder.class, CALLS_REAL_METHODS );
        databaseLayout = testDirectory.databaseLayout();
        when( dataSource.getDatabaseLayout() ).thenReturn( databaseLayout );
        indexListingMock = mock( NeoStoreFileIndexListing.class );
        when( indexListingMock.getIndexIds() ).thenReturn( new LongHashSet() );
        DatabaseFileListing storeFileListing = mock( DatabaseFileListing.class );
        when( storeFileListing.getNeoStoreFileIndexListing() ).thenReturn( indexListingMock );
        when( storeFileListing.builder() ).thenReturn( fileListingBuilder );
        when( dataSource.getDatabaseFileListing() ).thenReturn( storeFileListing );
        prepareStoreCopyFiles = new PrepareStoreCopyFiles( dataSource, fileSystemAbstraction );
    }

    @Test
    public void shouldHanldeEmptyListOfFilesForeEachType() throws Exception
    {
        setExpectedFiles( new StoreFileMetadata[0] );
        File[] files = prepareStoreCopyFiles.listReplayableFiles();
        StoreResource[] atomicFilesSnapshot = prepareStoreCopyFiles.getAtomicFilesSnapshot();
        assertEquals( 0, files.length );
        assertEquals( 0, atomicFilesSnapshot.length );
    }

    private void setExpectedFiles( StoreFileMetadata[] expectedFiles ) throws IOException
    {
        doAnswer( invocation -> Iterators.asResourceIterator( Iterators.iterator( expectedFiles ) ) ).when( fileListingBuilder ).build();
    }

    @Test
    public void shouldReturnExpectedListOfFileNamesForEachType() throws Exception
    {
        // given
        StoreFileMetadata[] expectedFiles = new StoreFileMetadata[]{new StoreFileMetadata( databaseLayout.file( "a" ), 1 ),
                new StoreFileMetadata( databaseLayout.file( "b" ), 2 )};
        setExpectedFiles( expectedFiles );

        //when
        File[] files = prepareStoreCopyFiles.listReplayableFiles();
        StoreResource[] atomicFilesSnapshot = prepareStoreCopyFiles.getAtomicFilesSnapshot();

        //then
        File[] expectedFilesConverted = Arrays.stream( expectedFiles ).map( StoreFileMetadata::file ).toArray( File[]::new );
        StoreResource[] exeptedAtomicFilesConverted = Arrays.stream( expectedFiles ).map(
                f -> new StoreResource( f.file(), getRelativePath( f ), f.recordSize(), fileSystemAbstraction ) ).toArray( StoreResource[]::new );
        assertArrayEquals( expectedFilesConverted, files );
        assertEquals( exeptedAtomicFilesConverted.length, atomicFilesSnapshot.length );
        for ( int i = 0; i < exeptedAtomicFilesConverted.length; i++ )
        {
            StoreResource expected = exeptedAtomicFilesConverted[i];
            StoreResource storeResource = atomicFilesSnapshot[i];
            assertEquals( expected.path(), storeResource.path() );
            assertEquals( expected.recordSize(), storeResource.recordSize() );
        }
    }

    @Test
    public void shouldHandleEmptyDescriptors()
    {
        LongSet indexIds = prepareStoreCopyFiles.getNonAtomicIndexIds();

        assertEquals( 0, indexIds.size() );
    }

    @Test
    public void shouldReturnEmptySetOfIdsAndIgnoreIndexListing()
    {
        LongSet expectedIndexIds = LongSets.immutable.of( 42 );
        when( indexListingMock.getIndexIds() ).thenReturn( expectedIndexIds );

        LongSet actualIndexIndexIds = prepareStoreCopyFiles.getNonAtomicIndexIds();

        assertTrue( actualIndexIndexIds.isEmpty() );
    }

    private String getRelativePath( StoreFileMetadata f )
    {
        try
        {
            return relativePath( databaseLayout.databaseDirectory(), f.file() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to create relative path" );
        }
    }
}
