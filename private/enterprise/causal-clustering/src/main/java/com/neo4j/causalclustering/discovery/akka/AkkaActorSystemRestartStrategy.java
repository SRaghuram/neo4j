/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public abstract class AkkaActorSystemRestartStrategy
{
    public abstract boolean restartRequired( Cluster cluster );

    public static class NeverRestart extends AkkaActorSystemRestartStrategy
    {

        @Override
        public boolean restartRequired( Cluster cluster )
        {
            return false;
        }
    }

    /**
     * This strategy is terrible because it basically prevents cluster formation
     */
    public static class AlwaysRestart extends AkkaActorSystemRestartStrategy
    {

        @Override
        public boolean restartRequired( Cluster cluster )
        {
            return true;
        }
    }

    public static class RestartIfAnyUnreachable extends AkkaActorSystemRestartStrategy
    {

        @Override
        public boolean restartRequired( Cluster cluster )
        {
            return cluster.state().getUnreachable().size() > 0;
        }
    }

    public static class RestartOnEvenMinutes extends AkkaActorSystemRestartStrategy
    {

        @Override
        public boolean restartRequired( Cluster cluster )
        {
            return Instant.now().getEpochSecond() % 120 <= 10;
        }
    }

    public static class RestartWhenMajorityUnreachable extends AkkaActorSystemRestartStrategy
    {

        @Override
        public boolean restartRequired( Cluster cluster )
        {
            ClusterEvent.CurrentClusterState clusterState = cluster.state();
            Set<Member> unreachable = clusterState.getUnreachable();
            Set<Member> upAndReachableMembers = new HashSet<>();

            int memberCount = 0;
            for ( Member m : clusterState.getMembers() )
            {
                memberCount++;
                if ( !unreachable.contains( m ) && m.status() == MemberStatus.up() )
                {
                    upAndReachableMembers.add( m );
                }
            }

            // Restart if we have been kicked out
            if ( memberCount == 0 && cluster.selfMember().status() == MemberStatus.removed() )
            {
                return true;
            }

            // Restart if we are a singleton
            if ( memberCount == 1 )
            {
                // This might prevent clusters forming at all
                return true;
            }

            return upAndReachableMembers.size() <= unreachable.size() && unreachable.size() > 0;
        }
    }
}
