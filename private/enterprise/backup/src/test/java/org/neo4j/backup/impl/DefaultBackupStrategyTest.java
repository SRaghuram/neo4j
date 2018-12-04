/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;
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
    private AddressResolver addressResolver = mock( AddressResolver.class );
    private AdvertisedSocketAddress resolvedFromAddress = new AdvertisedSocketAddress( "resolved-host", 1358 );

    private DefaultBackupStrategy strategy;

    private DatabaseLayout desiredBackupLayout = mock( DatabaseLayout.class );
    private Config config = mock( Config.class );
    private OptionalHostnamePort userProvidedAddress = new OptionalHostnamePort( (String) null, null, null );
    private StoreFiles storeFiles = mock( StoreFiles.class );
    private StoreId expectedStoreId = new StoreId( 11, 22, 33, 44 );

    @BeforeEach
    void setup() throws IOException, StoreIdDownloadFailedException
    {
        when( addressResolver.resolveCorrectAddress( any(), any() ) ).thenReturn( resolvedFromAddress );
        when( storeFiles.readStoreId( any() ) ).thenReturn( expectedStoreId );
        when( backupDelegator.fetchStoreId( any() ) ).thenReturn( expectedStoreId );
        strategy = new DefaultBackupStrategy( backupDelegator, addressResolver, NullLogProvider.getInstance(), storeFiles );
    }

    @Test
    void incrementalBackupsUseCorrectResolvedAddress() throws Exception
    {
        // given
        when( backupDelegator.tryCatchingUp( any(), any(), any() ) ).thenReturn( CatchupResult.SUCCESS_END_OF_STREAM );

        AdvertisedSocketAddress expectedAddress = new AdvertisedSocketAddress( "expected-host", 1298 );
        when( addressResolver.resolveCorrectAddress( any(), any() ) ).thenReturn( expectedAddress );

        // when
        strategy.performIncrementalBackup( desiredBackupLayout, config, userProvidedAddress );

        // then
        verify( backupDelegator ).tryCatchingUp( eq( expectedAddress ), any(), any() );
    }

    @Test
    void fullBackupUsesCorrectResolvedAddress() throws Exception
    {
        // given
        AdvertisedSocketAddress expectedAddress = new AdvertisedSocketAddress( "expected-host", 1578 );
        when( addressResolver.resolveCorrectAddress( any(), any() ) ).thenReturn( expectedAddress );

        // and ID of the local store can't be fetched
        doThrow( IOException.class ).when( storeFiles ).readStoreId( any() );

        // when
        strategy.performFullBackup( desiredBackupLayout, config, userProvidedAddress );

        // then
        verify( backupDelegator ).fetchStoreId( expectedAddress );
    }

    @Test
    void incrementalRunsCatchupWithTargetsStoreId() throws Exception
    {
        // given
        when( backupDelegator.tryCatchingUp( any(), any(), any() ) ).thenReturn( CatchupResult.SUCCESS_END_OF_STREAM );

        // when
        strategy.performIncrementalBackup( desiredBackupLayout, config, userProvidedAddress );

        // then
        verify( backupDelegator ).fetchStoreId( resolvedFromAddress );
        verify( backupDelegator ).tryCatchingUp( eq( resolvedFromAddress ), eq( expectedStoreId ), any() );
    }

    @Test
    void fullRunsRetrieveStoreWithTargetsStoreId() throws Exception
    {
        // given
        when( storeFiles.readStoreId( any() ) ).thenThrow( IOException.class );

        // when
        strategy.performFullBackup( desiredBackupLayout, config, userProvidedAddress );

        // then
        verify( backupDelegator ).fetchStoreId( resolvedFromAddress );
        verify( backupDelegator ).copy( resolvedFromAddress, expectedStoreId, desiredBackupLayout );
    }

    @Test
    void failingToRetrieveStoreIdCausesException_incrementalBackup() throws Exception
    {
        // given
        StoreIdDownloadFailedException storeIdDownloadFailedException = new StoreIdDownloadFailedException( "Expected description" );
        when( backupDelegator.fetchStoreId( any() ) ).thenThrow( storeIdDownloadFailedException );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, config, userProvidedAddress ) );

        // then
        assertEquals( storeIdDownloadFailedException, error.getCause() );
    }

    @Test
    void failingToCopyStoresCausesException_incrementalBackup() throws Exception
    {
        // given
        StoreCopyFailedException storeCopyFailedException = new StoreCopyFailedException( "Ops" );
        when( backupDelegator.tryCatchingUp( any(), eq( expectedStoreId ), any() ) ).thenThrow( storeCopyFailedException );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, config, userProvidedAddress ) );

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
                () -> strategy.performFullBackup( desiredBackupLayout, config, userProvidedAddress ) );

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
                () -> strategy.performFullBackup( desiredBackupLayout, config, userProvidedAddress ) );

        // then
        assertEquals( storeCopyFailedException, error.getCause() );
    }

    @Test
    void incrementalBackupsEndingInUnacceptedCatchupStateCauseException() throws Exception
    {
        // given
        CatchupResult unexpectedStatus = CatchupResult.E_STORE_UNAVAILABLE;
        when( backupDelegator.tryCatchingUp( any(), any(), any() ) ).thenReturn( unexpectedStatus );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performIncrementalBackup( desiredBackupLayout, config, userProvidedAddress ) );

        // then
        Throwable cause = error.getCause();
        assertThat( cause, instanceOf( StoreCopyFailedException.class ) );
        assertEquals( "End state of catchup was not a successful end of stream: " + unexpectedStatus, cause.getMessage() );
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
                () -> strategy.performIncrementalBackup( desiredBackupLayout, config, userProvidedAddress ) );

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
                () -> strategy.performIncrementalBackup( desiredBackupLayout, config, userProvidedAddress ) );

        // then
        assertEquals( StoreIdDownloadFailedException.class, error.getCause().getClass() );
    }

    @Test
    void fullBackupFailsWhenTargetHasStoreId() throws Exception
    {
        // given
        when( storeFiles.readStoreId( any() ) ).thenReturn( expectedStoreId );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class,
                () -> strategy.performFullBackup( desiredBackupLayout, config, userProvidedAddress ) );

        // then
        assertEquals( StoreIdDownloadFailedException.class, error.getCause().getClass() );
    }
}
