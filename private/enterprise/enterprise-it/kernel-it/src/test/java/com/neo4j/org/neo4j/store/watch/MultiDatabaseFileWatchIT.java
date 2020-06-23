/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.org.neo4j.store.watch;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.watcher.FileWatchEventListener;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.Files.delete;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.LogAssertions.assertThat;

@TestDirectoryExtension
class MultiDatabaseFileWatchIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private GraphDatabaseFacade firstContext;
    private GraphDatabaseFacade secondContext;
    private GraphDatabaseFacade thirdContext;
    private AssertableLogProvider logProvider;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() throws DatabaseExistsException
    {
        logProvider = new AssertableLogProvider( true );
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setInternalLogProvider( logProvider )
                .build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
        managementService.createDatabase( "first" );
        managementService.createDatabase( "second" );
        managementService.createDatabase( "third" );
        firstContext  = (GraphDatabaseFacade) managementService.database( "first" );
        secondContext = (GraphDatabaseFacade) managementService.database( "second" );
        thirdContext  = (GraphDatabaseFacade) managementService.database( "third" );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void deleteFileInOneDatabaseWarnAboutThatParticularDatabase() throws InterruptedException, IOException
    {
        Path firstDbMetadataStore = firstContext.databaseLayout().metadataStore();
        FileSystemWatcherService fileSystemWatcher = getFileSystemWatcher();
        DeletionLatchEventListener deletionListener = new DeletionLatchEventListener( firstDbMetadataStore.getFileName().toString() );
        fileSystemWatcher.getFileWatcher().addFileWatchEventListener( deletionListener );
        delete( firstDbMetadataStore );

        deletionListener.awaitDeletionNotification();

        assertThat( logProvider ).containsMessages( "'neostore' which belongs to the 'first' database was deleted while it was running." )
                                 .doesNotContainMessage( "'neostore' which belongs to the 'second' database was deleted while it was running." )
                                 .doesNotContainMessage( "'neostore' which belongs to the 'third' database was deleted while it was running." );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void differentEventsGoToDifferentDatabaseListeners() throws InterruptedException, IOException
    {
        Path firstDbMetadataStore = firstContext.databaseLayout().metadataStore();
        Path secondDbNodeStore = secondContext.databaseLayout().nodeStore();
        Path thirdDbRelStore = thirdContext.databaseLayout().relationshipStore();

        FileSystemWatcherService fileSystemWatcher = getFileSystemWatcher();
        DeletionLatchEventListener deletionListener =
                new DeletionLatchEventListener( thirdDbRelStore.getFileName().toString(), secondDbNodeStore.getFileName().toString(),
                        firstDbMetadataStore.getFileName().toString() );
        fileSystemWatcher.getFileWatcher().addFileWatchEventListener( deletionListener );

        delete( firstDbMetadataStore );
        delete( secondDbNodeStore );
        delete( thirdDbRelStore );

        deletionListener.awaitDeletionNotification();

        assertThat( logProvider ).containsMessages( "'neostore' which belongs to the 'first' database was deleted while it was running.",
                "'neostore.nodestore.db' which belongs to the 'second' database was deleted while it was running.",
                "'neostore.relationshipstore.db' which belongs to the 'third' database was deleted while it was running." );
    }

    private FileSystemWatcherService getFileSystemWatcher()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( FileSystemWatcherService.class );
    }

    private static class DeletionLatchEventListener implements FileWatchEventListener
    {
        private final Set<String> expectedFileNames;
        private final CountDownLatch deletionLatch;

        DeletionLatchEventListener( String... expectedFileNames )
        {
            this.expectedFileNames = Set.of( expectedFileNames );
            this.deletionLatch = new CountDownLatch( expectedFileNames.length );
        }

        @Override
        public void fileDeleted( WatchKey key, String fileName )
        {
            if ( expectedFileNames.contains( fileName ) )
            {
                deletionLatch.countDown();
            }
        }

        void awaitDeletionNotification() throws InterruptedException
        {
            deletionLatch.await();
        }
    }
}
