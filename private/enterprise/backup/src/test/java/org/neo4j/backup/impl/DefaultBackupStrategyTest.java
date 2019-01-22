/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultBackupStrategyTest
{
    private BackupDelegator backupDelegator = mock( BackupDelegator.class );
    private DatabaseLayout desiredBackupLayout = mock( DatabaseLayout.class );
    private AdvertisedSocketAddress address = new AdvertisedSocketAddress( "neo4j.com", 6362 );
    private StoreFiles storeFiles = mock( StoreFiles.class );
    private StoreId expectedStoreId = new StoreId( 11, 22, 33, 44 );
    private DefaultBackupStrategy strategy = new DefaultBackupStrategy( backupDelegator, NullLogProvider.getInstance(), storeFiles );

    @BeforeEach
    void setup() throws IOException, StoreIdDownloadFailedException
    {
        when( storeFiles.readStoreId( any() ) ).thenReturn( expectedStoreId );
        when( backupDelegator.fetchStoreId( any() ) ).thenReturn( expectedStoreId );
    }

    @Test
    void incrementalBackupsUseCorrectAddress() throws Exception
    {
        // given

        // when
        strategy.performIncrementalBackup( desiredBackupLayout, address );

        // then
        verify( backupDelegator ).tryCatchingUp( eq( address ), any( StoreId.class ), eq( desiredBackupLayout ) );
    }

    @Test
    void fullBackupUsesCorrectAddress() throws Exception
    {
        // given ID of the local store can't be fetched
        doThrow( IOException.class ).when( storeFiles ).readStoreId( any() );

        // when
        strategy.performFullBackup( desiredBackupLayout, address );

        // then
        verify( backupDelegator ).fetchStoreId( address );
    }

    @Test
    void incrementalRunsCatchupWithTargetsStoreId() throws Exception
    {
        // given

        // when
        strategy.performIncrementalBackup( desiredBackupLayout, address );

        // then
        verify( backupDelegator ).fetchStoreId( address );
        verify( backupDelegator ).tryCatchingUp( eq( address ), eq( expectedStoreId ), any() );
    }

    @Test
    void fullRunsRetrieveStoreWithTargetsStoreId() throws Exception
    {
        // given
        when( storeFiles.readStoreId( any() ) ).thenThrow( IOException.class );

        // when
        strategy.performFullBackup( desiredBackupLayout, address );

        // then
        verify( backupDelegator ).fetchStoreId( address );
        verify( backupDelegator ).copy( address, expectedStoreId, desiredBackupLayout );
    }

    @Test
    void failingToRetrieveStoreIdCausesException_incrementalBackup() throws Exception
    {
        // given
        StoreIdDownloadFailedException storeIdDownloadFailedException = new StoreIdDownloadFailedException( "Expected description" );
        when( backupDelegator.fetchStoreId( any() ) ).thenThrow( storeIdDownloadFailedException );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, address ) );

        // then
        assertEquals( storeIdDownloadFailedException, error.getCause() );
    }

    @Test
    void failingToCopyStoresCausesException_incrementalBackup() throws Exception
    {
        // given
        StoreCopyFailedException storeCopyFailedException = new StoreCopyFailedException( "Ops" );
        doThrow( storeCopyFailedException ).when( backupDelegator).tryCatchingUp( any(), eq( expectedStoreId ), any() );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, address ) );

        // then
        assertEquals( storeCopyFailedException, error.getCause() );
    }

    @Test
    void failingToRetrieveStoreIdCausesException_fullBackup() throws Exception
    {
        // given
        StoreIdDownloadFailedException storeIdDownloadFailedException = new StoreIdDownloadFailedException( "Expected description" );
        when( backupDelegator.fetchStoreId( any() ) ).thenThrow( storeIdDownloadFailedException );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performFullBackup( desiredBackupLayout, address ) );

        // then
        assertEquals( storeIdDownloadFailedException, error.getCause() );
    }

    @Test
    void failingToCopyStoresCausesException_fullBackup() throws Exception
    {
        // given
        StoreCopyFailedException storeCopyFailedException = new StoreCopyFailedException( "Oops" );
        doThrow( storeCopyFailedException ).when( backupDelegator ).copy( any(), any(), any() );

        // and
        when( storeFiles.readStoreId( any() ) ).thenThrow( IOException.class );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performFullBackup( desiredBackupLayout, address ) );

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
        when( storeFiles.readStoreId( any() ) ).thenThrow( IOException.class );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, address ) );

        // then
        assertThat( error.getCause(), instanceOf( StoreIdDownloadFailedException.class ) );
    }

    @Test
    void exceptionWhenStoreMismatch() throws Exception
    {
        // given
        when( storeFiles.readStoreId( any() ) ).thenReturn( new StoreId( 5, 4, 3, 2 ) );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, address ) );

        // then
        assertThat( error.getCause(), instanceOf( StoreIdDownloadFailedException.class ) );
    }

    @Test
    void fullBackupFailsWhenTargetHasStoreId() throws Exception
    {
        // given
        when( storeFiles.readStoreId( any() ) ).thenReturn( expectedStoreId );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performFullBackup( desiredBackupLayout, address ) );

        // then
        assertThat( error.getCause(), instanceOf( StoreIdDownloadFailedException.class ) );
    }
}
