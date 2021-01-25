/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.discovery.TopologyService;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class DatabaseTopologyNotifierTest
{
    @Test
    void shouldNotifyTopologyServiceOnStartAndStop()
    {
        // given
        var databaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
        var topologyService = mock( TopologyService.class );
        var notifier = new DatabaseTopologyNotifier( databaseId, topologyService );

        // when
        notifier.start();

        // then
        verify( topologyService ).onDatabaseStart( databaseId );
        verifyNoMoreInteractions( topologyService );

        // when
        notifier.stop();

        // then
        verify( topologyService ).onDatabaseStop( databaseId );
        verifyNoMoreInteractions( topologyService );
    }
}
