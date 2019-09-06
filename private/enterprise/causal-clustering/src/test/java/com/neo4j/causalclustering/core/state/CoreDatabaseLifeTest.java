/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.identity.BoundState;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith( {LifeExtension.class} )
class CoreDatabaseLifeTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @Inject
    private LifeSupport life;

    @Test
    void shouldNotifyTopologyServiceOnStart() throws Exception
    {
        var databaseId = databaseIdRepository.getRaw( "customers" );
        var topologyService = mock( CoreTopologyService.class );
        var coreDatabaseLife = createCoreDatabaseLife( databaseId, topologyService, life );

        coreDatabaseLife.init();
        coreDatabaseLife.start();

        verify( topologyService ).onDatabaseStart( databaseId );
    }

    @Test
    void shouldNotifyTopologyServiceOnStop() throws Exception
    {
        var databaseId = databaseIdRepository.getRaw( "orders" );
        var topologyService = mock( CoreTopologyService.class );
        var coreDatabaseLife = createCoreDatabaseLife( databaseId, topologyService, life );

        coreDatabaseLife.init();
        coreDatabaseLife.start();
        coreDatabaseLife.stop();

        var inOrder = inOrder( topologyService );
        inOrder.verify( topologyService ).onDatabaseStart( databaseId );
        inOrder.verify( topologyService ).onDatabaseStop( databaseId );
    }

    private static CoreDatabaseLife createCoreDatabaseLife( DatabaseId databaseId, CoreTopologyService topologyService, LifeSupport life ) throws Exception
    {
        var raftMachine = mock( RaftMachine.class );

        var database = mock( Database.class );
        when( database.getDatabaseId() ).thenReturn( databaseId );

        var raftBinder = mock( RaftBinder.class );
        when( raftBinder.bindToRaft() ).thenReturn( new BoundState( RaftIdFactory.random() ) );

        var applicationProcess = mock( CommandApplicationProcess.class );
        var messageHandler = mock( LifecycleMessageHandler.class );
        var snapshotService = mock( CoreSnapshotService.class );
        var downloaderService = mock( CoreDownloaderService.class );
        var recoveryFacade = mock( RecoveryFacade.class );
        var internalOperator = new ClusterInternalDbmsOperator();
        var panicService = mock( PanicService.class );

        return new CoreDatabaseLife( raftMachine, database, raftBinder, applicationProcess, messageHandler, snapshotService,
                downloaderService, recoveryFacade, life, internalOperator, topologyService, panicService );
    }
}
