/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.neo4j.kernel.database.DatabaseId;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class RaftCoreTopologyConnectorTest
{
    private static final DatabaseId DATABASE_ID = new DatabaseId( DEFAULT_DATABASE_NAME );

    @Test
    void shouldRegisterAsListenerWhenStarted()
    {
        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        RaftMachine raftMachine = mock( RaftMachine.class );
        RaftCoreTopologyConnector connector = new RaftCoreTopologyConnector( topologyService, raftMachine, DATABASE_ID );

        connector.start();

        InOrder inOrder = inOrder( topologyService, raftMachine );
        inOrder.verify( topologyService ).addLocalCoreTopologyListener( connector );
        inOrder.verify( raftMachine ).registerListener( connector );
    }

    @Test
    void shouldUnregisterAsListenerWhenStopped()
    {
        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        RaftMachine raftMachine = mock( RaftMachine.class );
        RaftCoreTopologyConnector connector = new RaftCoreTopologyConnector( topologyService, raftMachine, DATABASE_ID );

        connector.stop();

        InOrder inOrder = inOrder( topologyService, raftMachine );
        inOrder.verify( raftMachine ).unregisterListener( connector );
        inOrder.verify( topologyService ).removeLocalCoreTopologyListener( connector );
    }
}
