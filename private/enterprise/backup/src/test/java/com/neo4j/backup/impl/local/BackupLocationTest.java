/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.local;

import com.neo4j.backup.impl.MetadataStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.impl.local.DatabaseIdStore.getDatabaseFilePath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;

@DbmsExtension
@PageCacheExtension
class BackupLocationTest
{

    @Inject
    GraphDatabaseAPI graphDatabaseAPI;

    DatabaseLayout databaseLayout;

    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;
    private BackupLocation emptyBackupLocation;
    private Path emptyBackupDirectory;
    private BackupLocation neo4jBackupLocation;
    private StoreFiles storeFiles;

    @BeforeEach
    void setUp()
    {
        databaseLayout = graphDatabaseAPI.databaseLayout();
        storeFiles = new StoreFiles( fileSystem, pageCache );
        emptyBackupDirectory = testDirectory.directory( "non-existent" );
        emptyBackupLocation = new BackupLocation( DatabaseLayout.ofFlat( emptyBackupDirectory ), new StoreFiles( fileSystem, pageCache ),
                                                  new DatabaseIdStore( fileSystem ), new MetadataStore( fileSystem ),
                                                  new DefaultPageCacheTracer(), new FileManager( fileSystem ), NullLogProvider.getInstance() );

        neo4jBackupLocation = new BackupLocation( databaseLayout, storeFiles, new DatabaseIdStore( fileSystem ),
                                                  new MetadataStore( fileSystem ), PageCacheTracer.NULL, new FileManager( fileSystem ),
                                                  NullLogProvider.getInstance() );
    }

    @Test
    void emptyLocationForEmptyStoreBehavesCorrectly() throws IOException
    {
        assertThat( emptyBackupLocation )
                .matches( l ->
                {
                    try
                    {
                        return !l.hasExistingStore();
                    }
                    catch ( IOException e )
                    {
                        throw new UncheckedIOException( e );
                    }
                }, "should not have store" )
                .matches( l -> l.databaseId().isEmpty(), "database id is empty" );
        assertThat( emptyBackupLocation.storeId() ).isEmpty();

        assertThat( emptyBackupLocation.databaseDirectory() )
                .isEqualTo( emptyBackupDirectory );
        assertThat( emptyBackupLocation.isLocatedAt( emptyBackupLocation ) ).isTrue();
    }

    @Test
    void emptyLocationForNonEmptyStoreBehavesCorrectly() throws IOException
    {
        var expectedStoreId = storeFiles.readStoreId( databaseLayout, PageCursorTracer.NULL );

        assertThat( neo4jBackupLocation )
                .matches( backupLocation ->
                {
                    try
                    {
                        return backupLocation.hasExistingStore();
                    }
                    catch ( IOException e )
                    {
                        throw new UncheckedIOException( e );
                    }
                }, "should have store" );

        assertThat( neo4jBackupLocation.databaseId() )
                .isEmpty();
        assertThat( neo4jBackupLocation.storeId() )
                .contains( expectedStoreId );

        assertThat( neo4jBackupLocation.databaseDirectory() )
                .isEqualTo( databaseLayout.databaseDirectory() );

        assertThat( neo4jBackupLocation.isLocatedAt( neo4jBackupLocation ) )
                .isTrue();
    }

    @Test
    void backupLocationReactsToChangesInDatabaseIdStore() throws IOException
    {
        var randomDbId = DatabaseIdFactory.from( UUID.randomUUID() );
        var databaseIdStore = new DatabaseIdStore( fileSystem );
        var expectedDbId = graphDatabaseAPI.databaseId().databaseId();

        assertThat( neo4jBackupLocation.databaseId() )
                .isEmpty();

        neo4jBackupLocation.writeDatabaseId( expectedDbId );

        assertThat( databaseIdStore.readDatabaseId( databaseLayout.backupToolsFolder() ) )
                .isEqualTo( Optional.of( expectedDbId ) );

        assertThat( neo4jBackupLocation.databaseId() )
                .contains( expectedDbId );

        assertThat( neo4jBackupLocation.conflictsWith( expectedDbId ) ).isFalse();
        assertThat( neo4jBackupLocation.conflictsWith( randomDbId ) ).isTrue();
    }

    @Test
    void tryDeleteReturnsTrueOnSuccess() throws IOException
    {
        // given:

        var databaseIdStore = new DatabaseIdStore( fileSystem );
        var metadataStore = new MetadataStore( fileSystem );
        var fileManager = mock( FileManager.class );
        var neo4jBackupLocation =
                new BackupLocation( databaseLayout, storeFiles, databaseIdStore, metadataStore, PageCacheTracer.NULL,
                                    fileManager, NullLogProvider.getInstance() );

        // when:
        var deleteResult = neo4jBackupLocation.tryDelete();

        // then :
        assertThat( deleteResult ).isTrue();
        verify( fileManager ).deleteDir( eq( databaseLayout.databaseDirectory() ) );
    }

    @Test
    void tryDeleteReturnsFalseOnFailure() throws IOException
    {
        // given:
        var databaseIdStore = new DatabaseIdStore( fileSystem );
        var metadataStore = new MetadataStore( fileSystem );
        var fileManager = mock( FileManager.class );
        var neo4jBackupLocation =
                new BackupLocation( databaseLayout, storeFiles, databaseIdStore, metadataStore, PageCacheTracer.NULL,
                                    fileManager, NullLogProvider.getInstance() );

        doThrow( new IOException( "Failure to delete dir" ) ).when( fileManager ).deleteDir( any() );

        // when:
        var deleteResult = neo4jBackupLocation.tryDelete();

        // then :
        assertThat( deleteResult ).isFalse();
        verify( fileManager ).deleteDir( eq( databaseLayout.databaseDirectory() ) );
    }

    @Test
    void moveToCallsCopyDelete() throws IOException
    {
        // given:
        var fileManager = mock( FileManager.class );
        var databaseIdStore = new DatabaseIdStore( fileSystem );
        var metadataStore = new MetadataStore( fileSystem );
        var neo4jBackupLocation = new BackupLocation( databaseLayout, storeFiles, databaseIdStore, metadataStore, PageCacheTracer.NULL,
                                                      fileManager, NullLogProvider.getInstance() );

        var newDirectory = testDirectory.directory( "some-dir" );
        var dbDirectory = databaseLayout.databaseDirectory();
        assertThat( dbDirectory ).exists();

        // when:
        var newBackupLocation = neo4jBackupLocation.moveTo( newDirectory );

        //then:
        assertThat( newBackupLocation.databaseDirectory() ).isEqualTo( newDirectory );
        var backupLocation = neo4jBackupLocation.databaseDirectory();
        verify( fileManager, times( 1 ) ).copyDelete( eq( backupLocation ), eq( newDirectory ) );
    }

    @Test
    void shouldIdentifyStoreCorrectly() throws IOException
    {
        // given:
        var databaseIdStore = new DatabaseIdStore( fileSystem );
        var neo4jBackupLocation = new BackupLocation( databaseLayout, storeFiles, databaseIdStore, new MetadataStore( fileSystem ), PageCacheTracer.NULL,
                                                      new FileManager( fileSystem ), NullLogProvider.getInstance() );
        // then:
        assertThat( neo4jBackupLocation.hasSameStore( neo4jBackupLocation ) ).isTrue();
        assertThat( neo4jBackupLocation.hasSameStore( emptyBackupLocation ) ).isFalse();
    }

    @Test
    void shouldLogExceptionInCaseDatabaseIdFail()
    {
        // given
        final var databaseIdStore = mock( DatabaseIdStore.class );
        final var exception = new IllegalStateException( "No rights to read" );
        when( databaseIdStore.readDatabaseId( databaseLayout.backupToolsFolder() ) ).thenThrow( exception );

        final var logProvider = new AssertableLogProvider();
        var backupLocation = new BackupLocation( databaseLayout, storeFiles, databaseIdStore, new MetadataStore( fileSystem ), PageCacheTracer.NULL,
                                                 new FileManager( fileSystem ), logProvider );

        // when
        final var databaseId = backupLocation.databaseId();

        //then
        assertThat( databaseId ).isEmpty();
        LogAssertions.assertThat( logProvider )
                     .forClass( BackupLocation.class )
                     .forLevel( ERROR )
                     .containsMessages( "Error in reading databaseId file " + getDatabaseFilePath( databaseLayout.backupToolsFolder() ) );
    }
}
