/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.election;

import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.internal.helpers.collection.Iterables;

public class ElectionUtil
{
    private ElectionUtil()
    {
    }

    public static MemberId waitForLeaderAgreement( Iterable<RaftMachine> validRafts, long maxTimeMillis ) throws
            InterruptedException, TimeoutException
    {
        long viewCount = Iterables.count( validRafts );

        Map<MemberId, MemberId> leaderViews = new HashMap<>();
        CompletableFuture<MemberId> futureAgreedLeader = new CompletableFuture<>();

        Collection<Runnable> destructors = new ArrayList<>();
        for ( RaftMachine raft : validRafts )
        {
            destructors.add( leaderViewUpdatingListener( raft, validRafts, leaderViews, viewCount, futureAgreedLeader ) );
        }

        try
        {
            try
            {
                return futureAgreedLeader.get( maxTimeMillis, TimeUnit.MILLISECONDS );
            }
            catch ( ExecutionException e )
            {
                throw new RuntimeException( e );
            }
        }
        finally
        {
            destructors.forEach( Runnable::run );
        }
    }

    private static Runnable leaderViewUpdatingListener( RaftMachine raft, Iterable<RaftMachine>
            validRafts, Map<MemberId,MemberId> leaderViews, long viewCount, CompletableFuture<MemberId>
            futureAgreedLeader )
    {
        LeaderListener listener = newLeader ->
        {
            synchronized ( leaderViews )
            {
                leaderViews.put( raft.memberId(), newLeader.memberId() );

                boolean leaderIsValid = false;
                for ( RaftMachine validRaft : validRafts )
                {
                    if ( validRaft.memberId().equals( newLeader.memberId() ) )
                    {
                        leaderIsValid = true;
                    }
                }

                if ( newLeader.memberId() != null && leaderIsValid && allAgreeOnLeader( leaderViews, viewCount, newLeader.memberId() ) )
                {
                    futureAgreedLeader.complete( newLeader.memberId() );
                }
            }
        };

        raft.registerListener( listener );
        return () -> raft.unregisterListener( listener );
    }

    private static <T> boolean allAgreeOnLeader( Map<T,T> leaderViews, long viewCount, T leader )
    {
        if ( leaderViews.size() != viewCount )
        {
            return false;
        }

        for ( T leaderView : leaderViews.values() )
        {
            if ( !leader.equals( leaderView ) )
            {
                return false;
            }
        }

        return true;
    }
}
