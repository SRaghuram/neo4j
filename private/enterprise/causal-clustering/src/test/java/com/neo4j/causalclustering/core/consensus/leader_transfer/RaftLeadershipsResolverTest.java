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
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.identity.StubClusteringIdentityModule;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.assertj.core.api.Assertions.assertThat;

class RaftLeadershipsResolverTest
{
    private NamedDatabaseId fooId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
    private NamedDatabaseId barId = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
    private NamedDatabaseId bazId = DatabaseIdFactory.from( "baz", UUID.randomUUID() );

    @Test
    void shouldCorrectlyReturnCurrentLeadershipsForMember()
    {
        // given
        var identityModule = new StubClusteringIdentityModule();
        var myselfForFoo = identityModule.memberId( fooId );
        var myselfForBar = identityModule.memberId( barId );
        var remoteIdentityModule = new StubClusteringIdentityModule();
        var remoteForBaz = remoteIdentityModule.memberId( bazId );

        var databaseManager = new StubClusteredDatabaseManager();

        databaseWithLeader( databaseManager, myselfForFoo, fooId );
        databaseWithLeader( databaseManager, myselfForBar, barId );
        databaseWithLeader( databaseManager, remoteForBaz, bazId );

        var leadershipsResolver = new RaftLeadershipsResolver( databaseManager, identityModule );
        var otherLeadershipResolver = new RaftLeadershipsResolver( databaseManager, remoteIdentityModule );

        // when
        var leaderships = leadershipsResolver.myLeaderships();
        var otherLeaderships = otherLeadershipResolver.myLeaderships();

        // then
        assertThat( leaderships ).containsExactlyInAnyOrder( fooId, barId );
        assertThat( otherLeaderships ).containsOnly( bazId );
    }

    @Test
    void shouldHandleNoLeaderSituation()
    {
        // given
        var identityModule = new StubClusteringIdentityModule();
        var myselfForBar = identityModule.memberId( barId );

        var databaseManager = new StubClusteredDatabaseManager();

        databaseWithoutLeader( databaseManager, fooId );

        var leadershipsResolver = new RaftLeadershipsResolver( databaseManager, identityModule );

        // when
        var leaderships = leadershipsResolver.myLeaderships();

        // then
        assertThat( leaderships ).isEmpty();
        assertThat( databaseManager.registeredDatabases().keySet() ).contains( fooId );

        // when
        databaseWithLeader( databaseManager, myselfForBar, barId );

        leadershipsResolver = new RaftLeadershipsResolver( databaseManager, identityModule );

        leaderships = leadershipsResolver.myLeaderships();

        // then
        assertThat( leaderships ).doesNotContain( fooId ).containsExactly( barId );
        assertThat( databaseManager.registeredDatabases().keySet() ).contains( fooId );
    }

    private NamedDatabaseId databaseWithLeader( StubClusteredDatabaseManager databaseManager, RaftMemberId member, NamedDatabaseId dbId )
    {
        var leaderLocator = new StubLeaderLocator( new LeaderInfo( member, 0 ) );
        databaseManager.givenDatabaseWithConfig().withDatabaseId( dbId ).withLeaderLocator( leaderLocator ).register();
        return dbId;
    }

    private NamedDatabaseId databaseWithoutLeader( StubClusteredDatabaseManager databaseManager, NamedDatabaseId dbId )
    {
        var leaderLocator = new StubLeaderLocator( null );
        databaseManager.givenDatabaseWithConfig().withDatabaseId( dbId ).withLeaderLocator( leaderLocator ).register();
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
