/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.membership;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class MembershipWaiterLifecycle extends LifecycleAdapter
{
    private final MembershipWaiter membershipWaiter;
    private final long joinCatchupTimeout;
    private final RaftMachine raft;
    private final Log log;

    public MembershipWaiterLifecycle( MembershipWaiter membershipWaiter, long joinCatchupTimeout,
                                      RaftMachine raft, LogProvider logProvider )
    {
        this.membershipWaiter = membershipWaiter;
        this.joinCatchupTimeout = joinCatchupTimeout;
        this.raft = raft;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        CompletableFuture<Boolean> caughtUp = membershipWaiter.waitUntilCaughtUpMember( raft );

        try
        {
            caughtUp.get( joinCatchupTimeout, MILLISECONDS );
        }
        catch ( ExecutionException e )
        {
            log.error( "Server failed to join cluster", e.getCause() );
            throw e.getCause();
        }
        catch ( InterruptedException | TimeoutException e )
        {
            String message =
                    format( "Server failed to join cluster within catchup time limit [%d ms]", joinCatchupTimeout );
            log.error( message, e );
            throw new RuntimeException( message, e );
        }
        finally
        {
            caughtUp.cancel( true );
        }
    }
}
