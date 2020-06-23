/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.backup.impl.BackupExecutionException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.common.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.RANDOM_NUMBER;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;

@TestDirectoryExtension
@ExtendWith( {RandomExtension.class, SuppressOutputExtension.class} )
class OnlineBackupIT
{
    private static final String DB_NAME = DEFAULT_DATABASE_NAME;

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private RandomRule random;

    private Path backupsDir;
    private Path defaultDbBackupDir;
    private GraphDatabaseAPI db;
    private HostnamePort backupAddress;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        backupsDir = testDirectory.directory( "backups" ).toPath();
        defaultDbBackupDir = backupsDir.resolve( DB_NAME );

        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setConfig( online_backup_enabled, true )
                .build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );

        backupAddress = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class ).getLocalAddress( BACKUP_SERVER_NAME );

        writeRandomData();
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldPerformFullBackup()
    {
        OnlineBackup.Result result = executeBackupWithFallbackToFull();

        assertTrue( result.isConsistent() );

        assertEquals( DbRepresentation.of( db ), backupDbRepresentation() );
    }

    @Test
    void shouldPerformIncrementalBackup()
    {
        OnlineBackup.Result result1 = executeBackupWithFallbackToFull();
        assertTrue( result1.isConsistent() );

        writeRandomData();

        OnlineBackup.Result result2 = executeBackupWithoutFallbackToFull();
        assertTrue( result2.isConsistent() );

        assertEquals( DbRepresentation.of( db ), backupDbRepresentation() );
    }

    @Test
    void shouldPerformConsistencyCheckOnInconsistentDatabase()
    {
        corruptNodeStore();

        OnlineBackup.Result result = executeBackupWithFallbackToFull();

        assertFalse( result.isConsistent() );
    }

    @Test
    void shouldThrowWhenTargetDatabaseDoesNotExist()
    {
        RuntimeException error = assertThrows( RuntimeException.class, () ->
                OnlineBackup.from( backupAddress.getHost(), backupAddress.getPort() )
                        .backup( "unknown", backupsDir ) );

        assertThat( error.getCause(), instanceOf( BackupExecutionException.class ) );
    }

    @Test
    void shouldThrowWhenTargetDirectoryDoesNotExist()
    {
        RuntimeException error = assertThrows( RuntimeException.class, () ->
                OnlineBackup.from( backupAddress.getHost(), backupAddress.getPort() )
                        .backup( DB_NAME, backupsDir.resolve( "unknownDir" ) ) );

        assertThat( error.getCause(), instanceOf( BackupExecutionException.class ) );
    }

    @Test
    void shouldThrowWhenIncrementalBackupNotPossibleAndFallbackToFullNotAllowed() throws Exception
    {
        OnlineBackup.Result result1 = executeBackupWithFallbackToFull();
        assertTrue( result1.isConsistent() );

        corruptStoreIdInBackup(); // change store ID of the backup so that it looks like a different database

        RuntimeException error = assertThrows( RuntimeException.class, this::executeBackupWithoutFallbackToFull );
        assertThat( getRootCause( error ), instanceOf( StoreIdDownloadFailedException.class ) );
    }

    @Test
    void shouldFailWhenConfiguredWithInvalidPort()
    {
        // not possible to listen on port 0
        assertThrows( IllegalArgumentException.class, () -> OnlineBackup.from( "localhost", 0 ) );

        assertThrows( IllegalArgumentException.class, () -> OnlineBackup.from( "localhost", -1 ) );
        assertThrows( IllegalArgumentException.class, () -> OnlineBackup.from( "localhost", 99_000 ) );
    }

    private void corruptNodeStore()
    {
        RecordStorageEngine storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        NodeStore nodeStore = storageEngine.testAccessNeoStores().getNodeStore();
        nodeStore.scanAllRecords( record ->
        {
            record.setInUse( false );
            nodeStore.updateRecord( record, NULL );
            return false;
        }, NULL );
    }

    private void corruptStoreIdInBackup() throws IOException
    {
        Path backupDir = backupsDir.resolve( DB_NAME );
        PageCache pageCache = db.getDependencyResolver().resolveDependency( PageCache.class );
        File metadataStore = DatabaseLayout.ofFlat( backupDir ).metadataStore().toFile();

        // update store creation time and store random number
        MetaDataStore.setRecord( pageCache, metadataStore, TIME, random.nextInt(), NULL );
        MetaDataStore.setRecord( pageCache, metadataStore, RANDOM_NUMBER, random.nextInt(), NULL );
    }

    private OnlineBackup.Result executeBackupWithFallbackToFull()
    {
        return OnlineBackup.from( backupAddress.getHost(), backupAddress.getPort() )
                .withFallbackToFullBackup( true )
                .withConsistencyCheck( true )
                .withOutputStream( System.out )
                .backup( DB_NAME, backupsDir );
    }

    private OnlineBackup.Result executeBackupWithoutFallbackToFull()
    {
        return OnlineBackup.from( backupAddress.getHost(), backupAddress.getPort() )
                .withFallbackToFullBackup( false )
                .withConsistencyCheck( true )
                .withOutputStream( System.out )
                .backup( DB_NAME, backupsDir );
    }

    private DbRepresentation backupDbRepresentation()
    {
        return DbRepresentation.of( DatabaseLayout.ofFlat( defaultDbBackupDir ) );
    }

    private void writeRandomData()
    {
        Label label = label( random.nextAlphaNumericString() );
        String property = random.nextAlphaNumericString();
        RelationshipType relType = RelationshipType.withName( random.nextAlphaNumericString() );

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( label ).on( property ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, MINUTES );
            tx.commit();
        }

        int transactions = random.nextInt( 5, 20 );
        int nodesInTransaction = random.nextInt( 10, 50 );

        for ( int i = 0; i < transactions; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node previousNode = null;
                for ( int j = 0; j < nodesInTransaction; j++ )
                {
                    Node node = tx.createNode( label );
                    node.setProperty( property, random.nextString() );
                    if ( previousNode == null )
                    {
                        previousNode = node;
                    }
                    previousNode.createRelationshipTo( node, relType );
                    previousNode = node;
                }
                tx.commit();
            }
        }
    }
}
