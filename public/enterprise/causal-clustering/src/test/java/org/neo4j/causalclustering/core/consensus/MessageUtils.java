/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus;

import java.util.NoSuchElementException;
import java.util.function.Predicate;

import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.helpers.collection.Iterables;

import static java.lang.String.format;

public class MessageUtils
{
    private MessageUtils()
    {
    }

    public static RaftMessages.RaftMessage messageFor( Outcome outcome, final MemberId member )
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
