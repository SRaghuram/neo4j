/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;

interface RaftMembershipStateMachineEventHandler
{
    RaftMembershipStateMachineEventHandler onRole( Role role );

    RaftMembershipStateMachineEventHandler onRaftGroupCommitted();

    RaftMembershipStateMachineEventHandler onFollowerStateChange( FollowerStates<MemberId> followerStates );

    RaftMembershipStateMachineEventHandler onMissingMember( MemberId member );

    RaftMembershipStateMachineEventHandler onSuperfluousMember( MemberId member );

    RaftMembershipStateMachineEventHandler onTargetChanged( Set<MemberId> targetMembers );

    RaftMembershipStateMachineEventHandler onLeadershipTransfer( boolean areTransferring );

    void onExit();

    void onEntry();

    abstract class Adapter implements RaftMembershipStateMachineEventHandler
    {
        @Override
        public RaftMembershipStateMachineEventHandler onRole( Role role )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler onRaftGroupCommitted()
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler onMissingMember( MemberId member )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler onSuperfluousMember( MemberId member )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler onFollowerStateChange( FollowerStates<MemberId> followerStates )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler onTargetChanged( Set targetMembers )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler onLeadershipTransfer( boolean areTransferring )
        {
            return this;
        }

        @Override
        public void onExit() {}

        @Override
        public void onEntry() {}
    }
}
