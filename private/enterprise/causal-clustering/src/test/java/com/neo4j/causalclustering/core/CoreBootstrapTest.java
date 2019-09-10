/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.identity.BoundState;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.DatabaseStartAborter;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoreBootstrapTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @Test
    void shouldClearAborterCacheOnStart() throws Exception
    {
        // given
        var databaseStartAborter = mock( DatabaseStartAborter.class );
        when( databaseStartAborter.shouldAbort( any( NamedDatabaseId.class ) ) ).thenReturn( false );

        var databaseId = databaseIdRepository.getRaw( "products" );

        var raftBinder = mock( RaftBinder.class );
        when( raftBinder.bindToRaft( databaseStartAborter ) ).thenReturn( new BoundState( RaftIdFactory.random() ) );

        var bootstrap = createBootstrap( databaseId, databaseStartAborter, raftBinder );

        // when
        bootstrap.perform();

        // then
        var inOrder = inOrder( databaseStartAborter );
        inOrder.verify( databaseStartAborter ).started( databaseId );
    }

    @Test
    void shouldClearAborterCacheOnFailedStart() throws Exception
    {
        // given
        var databaseStartAborter = mock( DatabaseStartAborter.class );
        when( databaseStartAborter.shouldAbort( any( NamedDatabaseId.class ) ) ).thenReturn( false );

        var databaseId = databaseIdRepository.getRaw( "products" );
        var raftBinder = mock( RaftBinder.class );
        when( raftBinder.bindToRaft( databaseStartAborter ) ).thenThrow( new RuntimeException() );

        var bootstrap = createBootstrap( databaseId, databaseStartAborter, raftBinder );

        // when / then
        assertThrows( RuntimeException.class, bootstrap::perform );
        verify( databaseStartAborter ).started( databaseId );
    }

    private static CoreBootstrap createBootstrap( NamedDatabaseId namedDatabaseId, DatabaseStartAborter databaseStartAborter, RaftBinder raftBinder )
    {
        var database = mock( Database.class );
        when( database.getNamedDatabaseId() ).thenReturn( namedDatabaseId );

        var messageHandler = mock( LifecycleMessageHandler.class );
        var snapshotService = mock( CoreSnapshotService.class );
        var downloaderService = mock( CoreDownloaderService.class );
        var internalOperator = new ClusterInternalDbmsOperator();

        return new CoreBootstrap( database, raftBinder, messageHandler, snapshotService, downloaderService, internalOperator, databaseStartAborter );
    }
}
