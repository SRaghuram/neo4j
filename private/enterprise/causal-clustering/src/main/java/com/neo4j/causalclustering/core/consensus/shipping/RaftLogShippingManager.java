/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.shipping;

import com.neo4j.causalclustering.core.consensus.LeaderContext;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.core.consensus.outcome.ShipCommand;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Outbound;

import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class RaftLogShippingManager extends LifecycleAdapter implements RaftMembership.Listener
{
    private final Outbound<RaftMemberId,RaftMessages.RaftMessage> outbound;
    private final LogProvider logProvider;
    private final ReadableRaftLog raftLog;
    private final Clock clock;
    private final Lazy<RaftMemberId> myself;

    private final RaftMembership membership;
    private final long retryTimeMillis;
    private final int catchupBatchSize;
    private final int maxAllowedShippingLag;
    private final InFlightCache inFlightCache;

    private Map<RaftMemberId,RaftLogShipper> logShippers = new HashMap<>();
    private LeaderContext lastLeaderContext;

    private boolean running;
    private boolean stopped;
    private TimerService timerService;

    public RaftLogShippingManager( Outbound<RaftMemberId,RaftMessages.RaftMessage> outbound, LogProvider logProvider, ReadableRaftLog raftLog,
            TimerService timerService, Clock clock, Lazy<RaftMemberId> myself, RaftMembership membership, long retryTimeMillis,
            int catchupBatchSize, int maxAllowedShippingLag, InFlightCache inFlightCache )
    {
        this.outbound = outbound;
        this.logProvider = logProvider;
        this.raftLog = raftLog;
        this.timerService = timerService;
        this.clock = clock;
        this.myself = myself;
        this.membership = membership;
        this.retryTimeMillis = retryTimeMillis;
        this.catchupBatchSize = catchupBatchSize;
        this.maxAllowedShippingLag = maxAllowedShippingLag;
        this.inFlightCache = inFlightCache;
        membership.registerListener( this );
    }

    /**
     * Paused when stepping down from leader role.
     */
    public synchronized void pause()
    {
        running = false;

        logShippers.values().forEach( RaftLogShipper::stop );
        logShippers.clear();
    }

    /**
     * Resumed when becoming leader.
     */
    public synchronized void resume( LeaderContext initialLeaderContext )
    {
        if ( stopped )
        {
            return;
        }

        running = true;

        for ( RaftMemberId member : membership.replicationMembers() )
        {
            ensureLogShipperRunning( member, initialLeaderContext );
        }

        lastLeaderContext = initialLeaderContext;
    }

    @Override
    public synchronized void stop()
    {
        pause();
        stopped = true;
    }

    private void ensureLogShipperRunning( RaftMemberId member, LeaderContext leaderContext )
    {
        RaftLogShipper logShipper = logShippers.get( member );
        if ( logShipper == null && !member.equals( myself.get() ) )
        {
            logShipper = new RaftLogShipper( outbound, logProvider, raftLog, clock, timerService, myself.get(), member,
                    leaderContext.term, leaderContext.commitIndex, retryTimeMillis, catchupBatchSize,
                    maxAllowedShippingLag, inFlightCache );

            logShippers.put( member, logShipper );

            logShipper.start();
        }
    }

    public synchronized void handleCommands( Iterable<ShipCommand> shipCommands, LeaderContext leaderContext )
    {
        for ( ShipCommand shipCommand : shipCommands )
        {
            for ( RaftLogShipper logShipper : logShippers.values() )
            {
                shipCommand.applyTo( logShipper, leaderContext );
            }
        }

        lastLeaderContext = leaderContext;
    }

    @Override
    public synchronized void onMembershipChanged()
    {
        if ( lastLeaderContext == null || !running )
        {
            return;
        }

        HashSet<RaftMemberId> toBeRemoved = new HashSet<>( logShippers.keySet() );
        toBeRemoved.removeAll( membership.replicationMembers() );

        for ( RaftMemberId member : toBeRemoved )
        {
            RaftLogShipper logShipper = logShippers.remove( member );
            if ( logShipper != null )
            {
                logShipper.stop();
            }
        }

        for ( RaftMemberId replicationMember : membership.replicationMembers() )
        {
            ensureLogShipperRunning( replicationMember, lastLeaderContext );
        }
    }

    @Override
    public String toString()
    {
        return format( "RaftLogShippingManager{logShippers=%s, myself=%s}", logShippers, myself );
    }
}
