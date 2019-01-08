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
package org.neo4j.causalclustering.core.consensus.membership;

import java.time.Clock;

import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;

class CatchupGoal
{
    private static final long MAX_ROUNDS = 10;

    private final ReadableRaftLog raftLog;
    private final Clock clock;
    private final long electionTimeout;

    private long targetIndex;
    private long roundCount;
    private long startTime;

    CatchupGoal( ReadableRaftLog raftLog, Clock clock, long electionTimeout )
    {
        this.raftLog = raftLog;
        this.clock = clock;
        this.electionTimeout = electionTimeout;
        this.targetIndex = raftLog.appendIndex();
        this.startTime = clock.millis();

        this.roundCount = 1;
    }

    boolean achieved( FollowerState followerState )
    {
        if ( followerState.getMatchIndex() >= targetIndex )
        {
            if ( (clock.millis() - startTime) <= electionTimeout )
            {
                return true;
            }
            else if ( roundCount <  MAX_ROUNDS )
            {
                roundCount++;
                startTime = clock.millis();
                targetIndex = raftLog.appendIndex();
            }
        }
        return false;
    }
}
