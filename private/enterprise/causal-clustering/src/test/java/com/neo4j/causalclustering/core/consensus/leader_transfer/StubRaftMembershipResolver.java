/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Set;

import org.neo4j.kernel.database.NamedDatabaseId;

class StubRaftMembershipResolver implements RaftMembershipResolver
{
    private final Set<RaftMemberId> votingMembers;

    StubRaftMembershipResolver( RaftMemberId... members )
    {
        this.votingMembers = Set.of( members );
    }

    @Override
    public RaftMembership membersFor( NamedDatabaseId databaseId )
    {
        return new StubRaftMembership( votingMembers );
    }

    private static class StubRaftMembership implements RaftMembership
    {
        private final Set<RaftMemberId> memberIds;

        StubRaftMembership( Set<RaftMemberId> memberIds )
        {
            this.memberIds = memberIds;
        }

        @Override
        public Set<RaftMemberId> votingMembers()
        {
            return memberIds;
        }

        @Override
        public Set<RaftMemberId> replicationMembers()
        {
            return memberIds;
        }

        @Override
        public void registerListener( Listener listener )
        { // no-op
        }
    }
}
