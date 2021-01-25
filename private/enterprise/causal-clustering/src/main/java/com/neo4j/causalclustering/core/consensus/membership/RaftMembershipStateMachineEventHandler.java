/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Set;

interface RaftMembershipStateMachineEventHandler
{
    RaftMembershipStateMachineEventHandler onRole( Role role );

    RaftMembershipStateMachineEventHandler onRaftGroupCommitted();

    RaftMembershipStateMachineEventHandler onFollowerStateChange( FollowerStates<RaftMemberId> followerStates );

    RaftMembershipStateMachineEventHandler onMissingMember( RaftMemberId member );

    RaftMembershipStateMachineEventHandler onSuperfluousMember( RaftMemberId member );

    RaftMembershipStateMachineEventHandler onTargetChanged( Set<RaftMemberId> targetMembers );

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
        public RaftMembershipStateMachineEventHandler onMissingMember( RaftMemberId member )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler onSuperfluousMember( RaftMemberId member )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler onFollowerStateChange( FollowerStates<RaftMemberId> followerStates )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler onTargetChanged( Set<RaftMemberId> targetMembers )
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
