/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.state.BootstrapSaver;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.core.state.storage.InMemorySimpleStorage;
import com.neo4j.causalclustering.identity.BoundState;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.ClusterInternalDbmsOperator.BootstrappingHandle;
import com.neo4j.dbms.DatabaseStartAborter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CoreBootstrapTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final LifecycleMessageHandler messageHandler = mock( LifecycleMessageHandler.class );
    private final Database database = mock( Database.class );
    private final CoreSnapshotService snapshotService = mock( CoreSnapshotService.class );
    private final CoreDownloaderService downloaderService = mock( CoreDownloaderService.class );
    private final ClusterInternalDbmsOperator internalOperator = mock( ClusterInternalDbmsOperator.class );
    private final RaftBinder raftBinder = mock( RaftBinder.class );
    private final DatabaseStartAborter databaseStartAborter = mock( DatabaseStartAborter.class );
    private final BootstrappingHandle bootstrapHandle = mock( BootstrappingHandle.class );
    private final BootstrapSaver bootstrapSaver = mock( BootstrapSaver.class );
    private final TempBootstrapDir tempBootstrapDir = mock( TempBootstrapDir.class );

    private final NamedDatabaseId databaseId = databaseIdRepository.getRaw( "foo" );
    private final RaftId raftId = IdFactory.randomRaftId();
    private InMemorySimpleStorage raftIdStorage = mock(InMemorySimpleStorage.class);

    @BeforeEach
    void setup()
    {
        when( database.getNamedDatabaseId() ).thenReturn( databaseId );
        when( internalOperator.bootstrap( databaseId ) ).thenReturn( bootstrapHandle );
    }

    @Test
    void successfulStart() throws Exception
    {
        // given
        when( raftBinder.bindToRaft( databaseStartAborter ) ).thenReturn( new BoundState( raftId ) );

        var bootstrap = createBootstrap();

        // when
        bootstrap.perform();

        // then
        var inOrder = inOrder( bootstrapSaver, tempBootstrapDir, databaseStartAborter, messageHandler, internalOperator, bootstrapHandle, raftIdStorage );
        inOrder.verify( internalOperator ).bootstrap( databaseId );
        inOrder.verify( bootstrapSaver ).restore( any() );
        inOrder.verify( tempBootstrapDir ).delete();
        inOrder.verify( messageHandler ).start( raftId );
        inOrder.verify( raftIdStorage ).writeState( raftId );
        inOrder.verify( bootstrapHandle ).release();
        inOrder.verify( databaseStartAborter ).started( databaseId );
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void failBinding() throws Exception
    {
        // given
        when( raftBinder.bindToRaft( databaseStartAborter ) ).thenThrow( new RuntimeException() );

        var bootstrap = createBootstrap();

        // when
        assertThrows( RuntimeException.class, bootstrap::perform );

        // then
        var inOrder = inOrder( bootstrapSaver, tempBootstrapDir, databaseStartAborter, messageHandler, internalOperator, bootstrapHandle, raftIdStorage );
        inOrder.verify( internalOperator ).bootstrap( databaseId );
        inOrder.verify( bootstrapSaver ).restore( any() );
        inOrder.verify( tempBootstrapDir ).delete();
        inOrder.verify( bootstrapHandle ).release();
        inOrder.verify( databaseStartAborter ).started( databaseId );
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void failAwaitState() throws Exception
    {
        // given
        when( raftBinder.bindToRaft( databaseStartAborter ) ).thenReturn( new BoundState( raftId ) );
        doThrow( new DatabaseStartAbortedException( databaseId ) ).when( snapshotService ).awaitState( eq( databaseStartAborter ), any( Duration.class ) );

        var bootstrap = createBootstrap();

        // when
        assertThrows( DatabaseStartAbortedException.class, bootstrap::perform );

        // then
        var inOrder = inOrder( databaseStartAborter, messageHandler, internalOperator, bootstrapHandle, raftIdStorage );
        inOrder.verify( internalOperator ).bootstrap( databaseId );
        inOrder.verify( messageHandler ).start( raftId );
        inOrder.verify( messageHandler ).stop();
        inOrder.verify( bootstrapHandle ).release();
        inOrder.verify( databaseStartAborter ).started( databaseId );
        inOrder.verifyNoMoreInteractions();
    }

    private CoreBootstrap createBootstrap()
    {
        return new CoreBootstrap( database, raftBinder, messageHandler, snapshotService, downloaderService, internalOperator, databaseStartAborter,
                raftIdStorage, bootstrapSaver, tempBootstrapDir );
    }
}
