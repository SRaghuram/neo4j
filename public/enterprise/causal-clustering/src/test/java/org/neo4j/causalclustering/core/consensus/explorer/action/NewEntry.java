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
package org.neo4j.causalclustering.core.consensus.explorer.action;

import java.io.IOException;
import java.time.Clock;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.core.consensus.explorer.ClusterState;
import org.neo4j.causalclustering.identity.MemberId;

public class NewEntry implements Action
{
    private final MemberId member;
    private final Clock clock;

    public NewEntry( MemberId member, Clock clock )
    {
        this.member = member;
        this.clock = clock;
    }

    @Override
    public ClusterState advance( ClusterState previous ) throws IOException
    {
        ClusterState newClusterState = new ClusterState( previous );
        Queue<RaftMessages.RaftMessage> newQueue = new LinkedList<>( previous.queues.get( member ) );
        newQueue.offer( new RaftMessages.NewEntry.Request( member, new ReplicatedString(
                "content" ) ) );
        newClusterState.queues.put( member, newQueue );
        return newClusterState;
    }
}
