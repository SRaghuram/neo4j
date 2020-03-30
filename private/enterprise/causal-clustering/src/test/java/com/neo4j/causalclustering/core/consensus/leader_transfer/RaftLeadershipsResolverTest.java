package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.List;
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
        var myself = new MemberId( UUID.randomUUID() );
        var core1 = new MemberId( UUID.randomUUID() );

        var databaseManager = new StubClusteredDatabaseManager();

        var db1 = databaseWithLeader( databaseManager, myself, "foo" );
        var db2 = databaseWithLeader( databaseManager, myself, "bar" );
        var db3 = databaseWithLeader( databaseManager, core1, "baz" );

        var leadershipsResolver = new RaftLeadershipsResolver( databaseManager, myself );
        var otherLeadershipResolver = new RaftLeadershipsResolver( databaseManager, core1 );

        // when
        var leaderships = leadershipsResolver.myLeaderships();
        var otherLeaderships = otherLeadershipResolver.myLeaderships();

        // then
        assertThat( leaderships ).containsExactlyInAnyOrder( db1, db2 );
        assertThat( otherLeaderships ).containsOnly( db3 );
    }

    private NamedDatabaseId databaseWithLeader( StubClusteredDatabaseManager databaseManager, MemberId member, String databaseName )
    {
        var leaderLocator = new StubLeaderLocator( new LeaderInfo( member, 0 ) );
        NamedDatabaseId dbId = DatabaseIdFactory.from( databaseName, UUID.randomUUID() );
        databaseManager.givenDatabaseWithConfig()
                       .withDatabaseId( dbId )
                       .withLeaderLocator( leaderLocator )
                       .register();
        return dbId;
    }

    private static class StubLeaderLocator implements LeaderLocator
    {
        private LeaderInfo leaderInfo;

        StubLeaderLocator( LeaderInfo leaderInfo )
        {
            this.leaderInfo = leaderInfo;
        }

        @Override
        public LeaderInfo getLeaderInfo()
        {
            return leaderInfo;
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
