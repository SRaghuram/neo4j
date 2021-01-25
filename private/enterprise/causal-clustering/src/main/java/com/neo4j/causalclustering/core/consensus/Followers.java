/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Followers
{

    private Followers()
    {
    }

    // TODO: This method is inefficient.. we should not have to update this state by a complete
    // TODO: iteration each time. Instead it should be updated as a direct response to each
    // TODO: append response.
    public static <MEMBER> long quorumAppendIndex( Set<MEMBER> votingMembers, FollowerStates<MEMBER> states )
    {
        /*
         * Build up a map of tx id -> number of instances that have appended,
         * sorted by tx id.
         *
         * This allows us to then iterate backwards over the values in the map,
         * adding up a total count of how many have appended, until we reach a majority.
         * Once we do, the tx id at the current entry in the map will be the highest one
         * with a majority appended.
         */

        TreeMap</* txId */Long, /* numAppended */Integer> appendedCounts = new TreeMap<>();
        for ( MEMBER member : votingMembers )
        {
            long txId = states.get( member ).getMatchIndex();
            appendedCounts.merge( txId, 1, ( a, b ) -> a + b );
        }

        // Iterate over it until we find a majority
        int total = 0;
        for ( Map.Entry<Long, Integer> entry : appendedCounts.descendingMap().entrySet() )
        {
            total += entry.getValue();
            if ( MajorityIncludingSelfQuorum.isQuorum( votingMembers.size(), total ) )
            {
                return entry.getKey();
            }
        }

        // No majority for any appended entry
        return -1;
    }
}
