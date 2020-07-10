/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.StubClusteringIdentityModule;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.assertj.core.api.Assertions.assertThat;

class RaftLeadershipsResolverTest
{
    @Test
    void shouldCorrectlyReturnCurrentLeadershipsForMember()
    {
        // given
        var identityModule = new StubClusteringIdentityModule();
        var myself = identityModule.memberId();
        var remoteIdentityModule = new StubClusteringIdentityModule();
        var core1 = remoteIdentityModule.memberId();

        var databaseManager = new StubClusteredDatabaseManager();

        var db1 = databaseWithLeader( databaseManager, myself, "foo" );
        var db2 = databaseWithLeader( databaseManager, myself, "bar" );
        var db3 = databaseWithLeader( databaseManager, core1, "baz" );

        var leadershipsResolver = new RaftLeadershipsResolver( databaseManager, identityModule );
        var otherLeadershipResolver = new RaftLeadershipsResolver( databaseManager, remoteIdentityModule );

        // when
        var leaderships = leadershipsResolver.myLeaderships();
        var otherLeaderships = otherLeadershipResolver.myLeaderships();

        // then
        assertThat( leaderships ).containsExactlyInAnyOrder( db1, db2 );
        assertThat( otherLeaderships ).containsOnly( db3 );
    }

    @Test
    void shouldHandleNoLeaderSituation()
    {
        // given
        var identityModule = new StubClusteringIdentityModule();
        var myself = identityModule.memberId();

        var databaseManager = new StubClusteredDatabaseManager();

        var db1 = databaseWithoutLeader( databaseManager, "foo" );

        var leadershipsResolver = new RaftLeadershipsResolver( databaseManager, identityModule );

        // when
        var leaderships = leadershipsResolver.myLeaderships();

        // then
        assertThat( leaderships ).isEmpty();
        assertThat( databaseManager.registeredDatabases().keySet() ).contains( db1 );

        // when
        var db2 = databaseWithLeader( databaseManager, myself, "bar" );

        leadershipsResolver = new RaftLeadershipsResolver( databaseManager, identityModule );

        leaderships = leadershipsResolver.myLeaderships();

        // then
        assertThat( leaderships ).doesNotContain( db1 ).containsExactly( db2 );
        assertThat( databaseManager.registeredDatabases().keySet() ).contains( db1 );
    }

    private NamedDatabaseId databaseWithLeader( StubClusteredDatabaseManager databaseManager, MemberId member, String databaseName )
    {
        var leaderLocator = new StubLeaderLocator( new LeaderInfo( member, 0 ) );
        NamedDatabaseId dbId = DatabaseIdFactory.from( databaseName, UUID.randomUUID() );
        databaseManager.givenDatabaseWithConfig().withDatabaseId( dbId ).withLeaderLocator( leaderLocator ).register();
        return dbId;
    }

    private NamedDatabaseId databaseWithoutLeader( StubClusteredDatabaseManager databaseManager, String databaseName )
    {
        var leaderLocator = new StubLeaderLocator( null );
        NamedDatabaseId dbId = DatabaseIdFactory.from( databaseName, UUID.randomUUID() );
        databaseManager.givenDatabaseWithConfig()
                       .withDatabaseId( dbId )
                       .withLeaderLocator( leaderLocator )
                       .register();
        return dbId;
    }

    static class StubLeaderLocator implements LeaderLocator
    {
        private final LeaderInfo leaderInfo;

        StubLeaderLocator( LeaderInfo leaderInfo )
        {
            this.leaderInfo = leaderInfo;
        }

        @Override
        public Optional<LeaderInfo> getLeaderInfo()
        {
            return leaderInfo == null ? Optional.empty() : Optional.of( leaderInfo );
        }

        @Override
        public void registerListener( LeaderListener listener )
        { // no-op
        }

        @Override
        public void unregisterListener( LeaderListener listener )
        { // no-op
        }
    }
}
