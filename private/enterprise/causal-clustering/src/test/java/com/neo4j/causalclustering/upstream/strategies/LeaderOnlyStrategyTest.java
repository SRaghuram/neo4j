/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderOnlyStrategyTest
{
    @Test
    void ignoresSelf() throws UpstreamDatabaseSelectionException
    {
        // given
        ServerId myself = new ServerId( new UUID( 1234, 5678 ) );
        var groupName = new ServerGroupName( "groupName" );

        // and
        LeaderOnlyStrategy leaderOnlyStrategy = new LeaderOnlyStrategy();
        var topologyServiceNoRetriesStrategy = new TopologyServiceThatPrioritisesItself( myself, groupName )
        {
            @Override
            public RoleInfo lookupRole( NamedDatabaseId databaseId, ServerId serverId )
            {
                return RoleInfo.LEADER;
            }
        };
        leaderOnlyStrategy.inject( topologyServiceNoRetriesStrategy, Config.defaults(), NullLogProvider.getInstance(), myself );

        // when
        Optional<ServerId> resolved = leaderOnlyStrategy.upstreamServerForDatabase( new TestDatabaseIdRepository().defaultDatabase() );

        // then
        assertTrue( resolved.isPresent() );
        assertNotEquals( myself, resolved.get() );
    }
}
