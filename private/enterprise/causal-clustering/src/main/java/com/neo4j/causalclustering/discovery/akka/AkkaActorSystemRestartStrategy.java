/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.util.VisibleForTesting;

public abstract class AkkaActorSystemRestartStrategy
{
    private static final Duration DEFAULT_CHECK_FREQUENCY = Duration.ofSeconds( 10 );

    public Duration checkFrequency()
    {
        return DEFAULT_CHECK_FREQUENCY;
    }

    public abstract boolean restartRequired( Cluster cluster );

    public abstract String getReason();

    public static class NeverRestart extends AkkaActorSystemRestartStrategy
    {
        @Override
        public boolean restartRequired( Cluster cluster )
        {
            return false;
        }

        @Override
        public String getReason()
        {
            throw new IllegalStateException( "this reason should never be required" );
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

        @Override
        public String getReason()
        {
            return "AkkaActorSystemRestartStrategy: " + this.getClass() + ". Running every " + checkFrequency().toString();
        }
    }

    public static class RestartIfAnyUnreachable extends AkkaActorSystemRestartStrategy
    {

        @Override
        public boolean restartRequired( Cluster cluster )
        {
            return cluster.state().getUnreachable().size() > 0;
        }

        @Override
        public String getReason()
        {
            return "AkkaActorSystemRestartStrategy: " + this.getClass() + ". Unreachable node detected.";
        }
    }

    public static class RestartOnEvenMinutes extends AkkaActorSystemRestartStrategy
    {

        @Override
        public boolean restartRequired( Cluster cluster )
        {
            return Instant.now().getEpochSecond() % 120 <= checkFrequency().toSeconds();
        }

        @Override
        public String getReason()
        {
            return "AkkaActorSystemRestartStrategy: " + this.getClass() + ". Running every " + checkFrequency().toString();
        }
    }

    public static class RestartWhenMajorityUnreachableOrSingletonFirstSeed extends AkkaActorSystemRestartStrategy
    {

        // TODO: put this consecutive failure handling into a wrapper class
        private final int MAX_CONSECUTIVE_FAILURES = 3;
        private final AtomicInteger count = new AtomicInteger();

        /**
         * currentFailureFirstDetected contains the instant when the current series of consecutive failures first began.
         * This is necessary because we cannot rely on akka to keep a fixed time gap between consecutive executions of this function.
         * Akka _schedules_ this function with the desired interval but if there is contention or a GC/VM pause then those scheduled executions can pile up
         * resulting in the function being _executed_ with an undefined (potentially 0) time gap between executions (n.b. because this is used by an actor it is
         * not possible for this to be executed concurrently)
         */
        private final AtomicReference<Instant> currentFailureFirstDetected = new AtomicReference<>();

        private final Supplier<Optional<SocketAddress>> firstSeed;

        public RestartWhenMajorityUnreachableOrSingletonFirstSeed( RemoteMembersResolver membersResolver )
        {
            this.firstSeed = membersResolver::first;
        }

        @Override
        public boolean restartRequired( Cluster cluster )
        {
            if ( anyRequirementUnsatisfied( cluster ) )
            {
                Instant now = Instant.now();
                var failureFirstDetectedAt = currentFailureFirstDetected.compareAndExchange( null, now );
                Instant failAfter = (failureFirstDetectedAt == null ? now : failureFirstDetectedAt)
                        .plus( DEFAULT_CHECK_FREQUENCY.multipliedBy( MAX_CONSECUTIVE_FAILURES ) );

                return count.incrementAndGet() >= MAX_CONSECUTIVE_FAILURES && now.isAfter( failAfter );
            }
            else
            {
                count.set( 0 );
                currentFailureFirstDetected.set( null );
                return false;
            }
        }

        @Override
        public String getReason()
        {
            return "AkkaActorSystemRestartStrategy: " + this.getClass()
                   + ". Triggered after " + count.get() + " consecutive failures. Running every " + checkFrequency().toString();
        }

        @VisibleForTesting
        boolean anyRequirementUnsatisfied( Cluster cluster )
        {
            ClusterEvent.CurrentClusterState clusterState = cluster.state();
            Set<Member> unreachable = clusterState.getUnreachable();
            Set<Member> upAndReachableMembers = new HashSet<>();

            // fail if we have been kicked out
            if ( cluster.selfMember().status() == MemberStatus.removed() )
            {
                return true;
            }

            for ( Member m : clusterState.getMembers() )
            {
                if ( !unreachable.contains( m ) && m.status() == MemberStatus.up() )
                {
                    upAndReachableMembers.add( m );
                }
            }

            // fail if we are a singleton and we are the seed node
            if ( isFirstSeed( cluster.selfMember() ) && upAndReachableMembers.size() == 1 && upAndReachableMembers.contains( cluster.selfMember() ) )
            {
                return true;
            }

            // fail if majority are unreachable
            return unreachable.size() > 0 && unreachable.size() > upAndReachableMembers.size();
        }

        private boolean isFirstSeed( Member selfMember )
        {
            Optional<SocketAddress> firstSeed = this.firstSeed.get();
            return firstSeed.map( socketAddress ->
                    // first check everything is not null to avoid NPEs at runtime
                    socketAddress.getHostname() != null && socketAddress.getPort() > 0
                    && selfMember != null && selfMember.address() != null
                    && selfMember.address().port().exists( Objects::nonNull ) && selfMember.address().host().exists( Objects::nonNull )
                    // Now check that the java socket address matches the akka/scala address
                    // selfMember.address is a scala Int which is hard to compare with java primitives directly.
                    // That is why we are ToString-ing the port values before comparing.
                    && socketAddress.getHostname().equals( selfMember.address().host().get() )
                    && Integer.toString( socketAddress.getPort()).equals( selfMember.address().port().get().toString() )
            ).orElse( false );
        }
    }
}
