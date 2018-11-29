/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.org.neo4j.store.watch;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.nio.file.WatchKey;
import java.util.concurrent.CountDownLatch;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.watcher.FileWatchEventListener;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.io.fs.FileUtils.deleteFile;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseFileWatchIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private DatabaseContext firstContext;
    private DatabaseContext secondContext;
    private DatabaseContext thirdContext;
    private AssertableLogProvider logProvider;

    @BeforeEach
    void setUp()
    {
        logProvider = new AssertableLogProvider( true );
        database = new TestCommercialGraphDatabaseFactory().setInternalLogProvider( logProvider ).newEmbeddedDatabase( testDirectory.storeDir() );
        DatabaseManager databaseManager = getDatabaseManager();
        firstContext = databaseManager.createDatabase( "first" );
        secondContext = databaseManager.createDatabase( "second" );
        thirdContext = databaseManager.createDatabase( "third" );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void deleteFileInOneDatabaseWarnAboutThatParticularDatabase()
    {
        assertTimeoutPreemptively( ofSeconds( 60 ), () ->
        {
            File firstDbMetadataStore = firstContext.getDatabase().getDatabaseLayout().metadataStore();
            FileSystemWatcherService fileSystemWatcher = getFileSystemWatcher();
            DeletionLatchEventListener deletionListener = new DeletionLatchEventListener( firstDbMetadataStore.getName() );
            fileSystemWatcher.getFileWatcher().addFileWatchEventListener( deletionListener );
            deleteFile( firstDbMetadataStore );

            deletionListener.awaitDeletionNotification();

            logProvider.assertLogStringContains( "'neostore' which belongs to the 'first' database was deleted while it was running." );
            logProvider.assertNoMessagesContaining( "'neostore' which belongs to the 'second' database was deleted while it was running." );
            logProvider.assertNoMessagesContaining( "'neostore' which belongs to the 'third' database was deleted while it was running." );
        } );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void differentEventsGoToDifferentDatabaseListeners()
    {
        assertTimeoutPreemptively( ofSeconds( 60 ), () ->
        {
            File firstDbMetadataStore = firstContext.getDatabase().getDatabaseLayout().metadataStore();
            File secondDbNodeStore = secondContext.getDatabase().getDatabaseLayout().nodeStore();
            File thirdDbRelStore = thirdContext.getDatabase().getDatabaseLayout().relationshipStore();

            FileSystemWatcherService fileSystemWatcher = getFileSystemWatcher();
            DeletionLatchEventListener deletionListener = new DeletionLatchEventListener( thirdDbRelStore.getName() );
            fileSystemWatcher.getFileWatcher().addFileWatchEventListener( deletionListener );

            deleteFile( firstDbMetadataStore );
            deleteFile( secondDbNodeStore );
            deleteFile( thirdDbRelStore );

            deletionListener.awaitDeletionNotification();

            logProvider.assertLogStringContains( "'neostore' which belongs to the 'first' database was deleted while it was running." );
            logProvider.assertLogStringContains( "'neostore.nodestore.db' which belongs to the 'second' database was deleted while it was running." );
            logProvider.assertLogStringContains(
                    "'neostore.relationshipstore.db' which belongs to the 'third' database was deleted while it was running." );
        } );
    }

    private FileSystemWatcherService getFileSystemWatcher()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( FileSystemWatcherService.class );
    }

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    private static class DeletionLatchEventListener implements FileWatchEventListener
    {
        private final String expectedFileName;
        private final CountDownLatch deletionLatch = new CountDownLatch( 1 );

        DeletionLatchEventListener( String expectedFileName )
        {
            this.expectedFileName = expectedFileName;
        }

        @Override
        public void fileDeleted( WatchKey key, String fileName )
        {
            if ( fileName.endsWith( expectedFileName ) )
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
