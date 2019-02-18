/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class LeaderOnlyStrategyTest
{
    @Test
    public void ignoresSelf() throws UpstreamDatabaseSelectionException
    {
        // given
        MemberId myself = new MemberId( new UUID( 1234, 5678 ) );
        String groupName = "groupName";

        // and
        LeaderOnlyStrategy leaderOnlyStrategy = new LeaderOnlyStrategy();
        TopologyServiceThatPrioritisesItself topologyServiceNoRetriesStrategy = new TopologyServiceThatPrioritisesItself( myself, groupName )
        {
            @Override
            public Map<MemberId,RoleInfo> allCoreRoles()
            {
                Map<MemberId,RoleInfo> roles = new HashMap<>();
                roles.put( myself, RoleInfo.LEADER );
                roles.put( coreNotSelf, RoleInfo.LEADER );
                return roles;
            }
        };
        leaderOnlyStrategy.inject( topologyServiceNoRetriesStrategy, Config.defaults(), NullLogProvider.getInstance(), myself );

        // when
        Optional<MemberId> resolved = leaderOnlyStrategy.upstreamDatabase();

        // then
        assertTrue( resolved.isPresent() );
        assertNotEquals( myself, resolved.get() );
    }
}
