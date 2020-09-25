/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.storecopy.DatabaseIdDownloadFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

class DefaultBackupStrategyTest
{
    private final BackupDelegator backupDelegator = mock( BackupDelegator.class );
    private final DatabaseLayout desiredBackupLayout = mock( DatabaseLayout.class );
    private final SocketAddress address = new SocketAddress( "neo4j.com", 6362 );
    private final StoreFiles storeFiles = mock( StoreFiles.class );
    private final StoreId expectedStoreId = new StoreId( 11, 22, 33, 44, 55 );
    private final NamedDatabaseId namedDatabaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
    private final DatabaseIdStore databaseIdStore = mock( DatabaseIdStore.class );
    private final DefaultBackupStrategy strategy =
            new DefaultBackupStrategy( backupDelegator, NullLogProvider.getInstance(), storeFiles, NULL, databaseIdStore );
    private final String databaseName = "database name";

    @BeforeEach
    void setup() throws IOException, StoreIdDownloadFailedException, DatabaseIdDownloadFailedException
    {
        when( storeFiles.readStoreId( any(), any() ) ).thenReturn( expectedStoreId );
        when( backupDelegator.fetchDatabaseId( any(), anyString() ) ).thenReturn( namedDatabaseId );
        when( backupDelegator.fetchStoreId( any(), any() ) ).thenReturn( expectedStoreId );
    }

    @Test
    void incrementalBackupsUseCorrectAddress() throws Exception
    {
        // given

        // when
        strategy.performIncrementalBackup( desiredBackupLayout, address, databaseName );

        // then
        verify( backupDelegator ).fetchDatabaseId( eq( address ), eq( databaseName ) );
        verify( backupDelegator ).tryCatchingUp( eq( address ), any( StoreId.class ), eq( namedDatabaseId ), eq( desiredBackupLayout ) );
    }

    @Test
    void fullBackupUsesCorrectAddress() throws Exception
    {
        // given ID of the local store can't be fetched
        doThrow( IOException.class ).when( storeFiles ).readStoreId( any(), any() );

        // when
        strategy.performFullBackup( desiredBackupLayout, address, databaseName );

        // then
        verify( backupDelegator ).fetchDatabaseId( address, databaseName );
        verify( backupDelegator ).fetchStoreId( address, namedDatabaseId );
    }

    @Test
    void incrementalRunsCatchupWithTargetsStoreId() throws Exception
    {
        // given

        // when
        strategy.performIncrementalBackup( desiredBackupLayout, address, databaseName );

        // then
        verify( backupDelegator ).fetchDatabaseId( address, databaseName );
        verify( backupDelegator ).fetchStoreId( address, namedDatabaseId );
        verify( backupDelegator ).tryCatchingUp( eq( address ), eq( expectedStoreId ), eq( namedDatabaseId ), any() );
    }

    @Test
    void fullRunsRetrieveStoreWithTargetsStoreId() throws Exception
    {
        // given
        when( storeFiles.readStoreId( any(), any() ) ).thenThrow( IOException.class );

        // when
        strategy.performFullBackup( desiredBackupLayout, address, databaseName );

        // then
        verify( backupDelegator ).fetchDatabaseId( address, databaseName );
        verify( backupDelegator ).fetchStoreId( address, namedDatabaseId );
        verify( backupDelegator ).copy( address, expectedStoreId, namedDatabaseId, desiredBackupLayout );
    }

    @Test
    void failingToRetrieveDatabaseIdCausesException_incrementalBackup() throws Exception
    {
        // given
        DatabaseIdDownloadFailedException exception = new DatabaseIdDownloadFailedException( "oops" );
        when( backupDelegator.fetchDatabaseId( any(), anyString() ) ).thenThrow( exception );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, address, databaseName ) );

        // then
        assertEquals( exception, error.getCause() );
    }

    @Test
    void failingToRetrieveStoreIdCausesException_incrementalBackup() throws Exception
    {
        // given
        StoreIdDownloadFailedException storeIdDownloadFailedException = new StoreIdDownloadFailedException( "Expected description" );
        when( backupDelegator.fetchStoreId( any(), any() ) ).thenThrow( storeIdDownloadFailedException );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, address, databaseName ) );

        // then
        assertEquals( storeIdDownloadFailedException, error.getCause() );
    }

    @Test
    void failingToCopyStoresCausesException_incrementalBackup() throws Exception
    {
        // given
        StoreCopyFailedException storeCopyFailedException = new StoreCopyFailedException( "Ops" );
        doThrow( storeCopyFailedException ).when( backupDelegator).tryCatchingUp( any(), eq( expectedStoreId ), any(), any() );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, address, databaseName ) );

        // then
        assertEquals( storeCopyFailedException, error.getCause() );
    }

    @Test
    void failingToRetrieveDatabaseIdCausesException_fullBackup() throws Exception
    {
        // given
        DatabaseIdDownloadFailedException exception = new DatabaseIdDownloadFailedException( "oops" );
        when( backupDelegator.fetchDatabaseId( any(), anyString() ) ).thenThrow( exception );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performFullBackup( desiredBackupLayout, address, databaseName ) );

        // then
        assertEquals( exception, error.getCause() );
    }

    @Test
    void failingToRetrieveStoreIdCausesException_fullBackup() throws Exception
    {
        // given
        StoreIdDownloadFailedException storeIdDownloadFailedException = new StoreIdDownloadFailedException( "Expected description" );
        when( backupDelegator.fetchStoreId( any(), any() ) ).thenThrow( storeIdDownloadFailedException );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performFullBackup( desiredBackupLayout, address, databaseName ) );

        // then
        assertEquals( storeIdDownloadFailedException, error.getCause() );
    }

    @Test
    void failingToCopyStoresCausesException_fullBackup() throws Exception
    {
        // given
        StoreCopyFailedException storeCopyFailedException = new StoreCopyFailedException( "Oops" );
        doThrow( storeCopyFailedException ).when( backupDelegator ).copy( any(), any(), any(), any() );

        // and
        when( storeFiles.readStoreId( any(), any() ) ).thenThrow( IOException.class );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performFullBackup( desiredBackupLayout, address, databaseName ) );

        // then
        assertEquals( storeCopyFailedException, error.getCause() );
    }

    @Test
    void lifecycleDelegatesToNecessaryServices() throws Throwable
    {
        // when
        strategy.start();

        // then
        verify( backupDelegator ).start();
        verify( backupDelegator, never() ).stop();

        // when
        strategy.stop();

        // then
        verify( backupDelegator ).start(); // still total 1 calls
        verify( backupDelegator ).stop();
    }

    @Test
    void exceptionWhenStoreMismatchNoExistingBackup() throws Exception
    {
        // given
        when( storeFiles.readStoreId( any(), any() ) ).thenThrow( IOException.class );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, address, databaseName ) );

        // then
        assertThat( error.getCause(), instanceOf( StoreIdDownloadFailedException.class ) );
    }

    @Test
    void exceptionWhenStoreMismatch() throws Exception
    {
        // given
        when( storeFiles.readStoreId( any(), any() ) ).thenReturn( new StoreId( 5, 4, 3, 2, 1 ) );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, address, databaseName ) );

        // then
        assertThat( error.getCause(), instanceOf( StoreIdDownloadFailedException.class ) );
    }

    @Test
    void fullBackupFailsWhenTargetHasStoreId() throws Exception
    {
        // given
        when( storeFiles.readStoreId( any(), any() ) ).thenReturn( expectedStoreId );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performFullBackup( desiredBackupLayout, address, databaseName ) );

        // then
        assertThat( error.getCause(), instanceOf( StoreIdDownloadFailedException.class ) );
    }
}
