/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.NoSuchElementException;
import java.util.function.Predicate;

import org.neo4j.internal.helpers.collection.FilteringIterable;
import org.neo4j.internal.helpers.collection.Iterables;

import static java.lang.String.format;

public class MessageUtils
{
    private MessageUtils()
    {
    }

    public static RaftMessages.RaftMessage messageFor( Outcome outcome, final RaftMemberId member )
    {
        Predicate<RaftMessages.Directed> selectMember = message -> message.to() == member;
        try
        {
            return Iterables.single( new FilteringIterable<>( outcome.getOutgoingMessages(), selectMember ) )
                        .message();
        }
        catch ( NoSuchElementException e )
        {
            throw new AssertionError( format( "Expected message for %s, but outcome only contains %s.",
                    member, outcome.getOutgoingMessages() ) );
        }
    }
}
