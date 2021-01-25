/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.election;

import com.neo4j.causalclustering.core.consensus.RaftMachineBuilder;
import com.neo4j.causalclustering.core.consensus.RaftMachineBuilder.RaftFixture;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import com.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import com.neo4j.causalclustering.messaging.TestNetwork;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.function.Predicates;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.internal.helpers.collection.Iterables.asSet;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class Fixture
{
    private final Set<RaftMemberId> members = new HashSet<>();
    private final Set<BootstrapWaiter> bootstrapWaiters = new HashSet<>();
    private final List<TimerService> timerServices = new ArrayList<>();
    private final JobScheduler scheduler = createInitialisedScheduler();
    final Set<RaftFixture> rafts = new HashSet<>();
    final TestNetwork net;

    Fixture( Set<RaftMemberId> memberIds, TestNetwork net, long electionTimeout, long heartbeatInterval, Clock clock )
    {
        this.net = net;

        for ( RaftMemberId member : memberIds )
        {
            TestNetwork.Inbound inbound = net.new Inbound( member );
            TestNetwork.Outbound outbound = net.new Outbound( member );

            members.add( member );

            TimerService timerService = createTimerService();

            BootstrapWaiter waiter = new BootstrapWaiter();
            bootstrapWaiters.add( waiter );

            InMemoryRaftLog raftLog = new InMemoryRaftLog();
            RaftFixture raftFixture =
                    new RaftMachineBuilder( member, memberIds.size(), RaftTestMemberSetBuilder.INSTANCE, clock )
                            .electionTimeout( electionTimeout )
                            .heartbeatInterval( heartbeatInterval )
                            .inbound( inbound )
                            .outbound( outbound )
                            .timerService( timerService )
                            .raftLog( raftLog )
                            .commitListener( waiter )
                            .buildFixture();

            rafts.add( raftFixture );
        }
    }

    private TimerService createTimerService()
    {
        TimerService timerService = new TimerService( scheduler, NullLogProvider.getInstance() );
        timerServices.add( timerService );
        return timerService;
    }

    void boot() throws Throwable
    {
        scheduler.start();
        for ( RaftFixture raft : rafts )
        {
            raft.raftLog().append( new RaftLogEntry( 0, new MemberIdSet( asSet( members ) ) ) );
            raft.raftMachine().installCoreState( new RaftCoreState( new MembershipEntry( 0, members ) ) );
            raft.raftMachine().postRecoveryActions();
        }
        net.start();
        awaitBootstrapped();
    }

    public void tearDown() throws Throwable
    {
        net.stop();
        for ( RaftFixture raft : rafts )
        {
            raft.logShipping().stop();
        }
        scheduler.stop();
        scheduler.shutdown();
    }

    /**
     * This class simply waits for a single entry to have been committed,
     * which should be the initial member set entry.
     *
     * If all members of the cluster have committed such an entry, it's possible for any member
     * to perform elections. We need to meet this condition before we start disconnecting members.
     */
    private static class BootstrapWaiter implements RaftMachineBuilder.CommitListener
    {
        private AtomicBoolean bootstrapped = new AtomicBoolean( false );

        @Override
        public void notifyCommitted( long commitIndex )
        {
            if ( commitIndex >= 0 )
            {
                bootstrapped.set( true );
            }
        }
    }

    private void awaitBootstrapped() throws TimeoutException
    {
        Predicates.await( () ->
        {
            for ( BootstrapWaiter bootstrapWaiter : bootstrapWaiters )
            {
                if ( !bootstrapWaiter.bootstrapped.get() )
                {
                    return false;
                }
            }
            return true;
        }, 30, SECONDS, 100, MILLISECONDS );
    }
}
