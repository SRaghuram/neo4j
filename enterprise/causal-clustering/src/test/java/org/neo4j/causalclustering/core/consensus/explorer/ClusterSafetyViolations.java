/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus.explorer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.RaftMessageHandler;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.roles.Leader;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.identity.MemberId;

import static org.neo4j.causalclustering.core.consensus.log.RaftLogHelper.readLogEntry;

public class ClusterSafetyViolations
{
    public static List<Violation> violations( ClusterState state ) throws IOException
    {
        List<Violation> invariantsViolated = new ArrayList<>();

        if ( multipleLeadersInSameTerm( state ) )
        {
            invariantsViolated.add( Violation.MULTIPLE_LEADERS );
        }

        if ( inconsistentCommittedLogEntries( state ) )
        {
            invariantsViolated.add( Violation.DIVERGED_LOG );
        }

        return invariantsViolated;
    }

    public static boolean inconsistentCommittedLogEntries( ClusterState state ) throws IOException
    {
        int index = 0;
        boolean moreLog = true;
        while ( moreLog )
        {
            moreLog = false;
            RaftLogEntry clusterLogEntry = null;
            for ( ComparableRaftState memberState : state.states.values() )
            {
                if ( index <= memberState.commitIndex() )
                {
                    RaftLogEntry memberLogEntry = readLogEntry( memberState.entryLog(), index );
                    if ( clusterLogEntry == null )
                    {
                        clusterLogEntry = memberLogEntry;
                    }
                    else
                    {
                        if ( !clusterLogEntry.equals( memberLogEntry ) )
                        {
                            return true;
                        }
                    }
                }
                if ( index < memberState.commitIndex() )
                {
                    moreLog = true;
                }
            }
            index++;
        }
        return false;
    }

    public static boolean multipleLeadersInSameTerm( ClusterState state )
    {
        Set<Long> termThatHaveALeader = new HashSet<>();
        for ( Map.Entry<MemberId, Role> entry : state.roles.entrySet() )
        {
            RaftMessageHandler role = entry.getValue().handler;
            if ( role instanceof Leader )
            {
                long term = state.states.get( entry.getKey() ).term();
                if ( termThatHaveALeader.contains( term ) )
                {
                    return true;
                }
                else
                {
                    termThatHaveALeader.add( term );
                }
            }
        }
        return false;
    }

    public enum Violation
    {
        DIVERGED_LOG, MULTIPLE_LEADERS
    }

}
