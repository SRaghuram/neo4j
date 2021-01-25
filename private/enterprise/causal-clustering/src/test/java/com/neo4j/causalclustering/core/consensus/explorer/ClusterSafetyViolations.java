/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.explorer;

import com.neo4j.causalclustering.core.consensus.RaftMessageHandler;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.RaftLogHelper;
import com.neo4j.causalclustering.core.consensus.roles.Leader;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterSafetyViolations
{
    private ClusterSafetyViolations()
    {
    }

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
                    RaftLogEntry memberLogEntry = RaftLogHelper.readLogEntry( memberState.entryLog(), index );
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
        for ( Map.Entry<RaftMemberId, Role> entry : state.roles.entrySet() )
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
