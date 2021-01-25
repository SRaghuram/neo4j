/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.directory;

import akka.cluster.ddata.AbstractReplicatedData;
import akka.cluster.ddata.ReplicatedData;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;

/**
 * Simple wrapper around {@link LeaderInfo} to implement {@link ReplicatedData} and provide a custom merge function.
 */
public class ReplicatedLeaderInfo extends AbstractReplicatedData<ReplicatedLeaderInfo>
{
    private final LeaderInfo leaderInfo;

    public ReplicatedLeaderInfo( LeaderInfo leaderInfo )
    {
        this.leaderInfo = leaderInfo;
    }

    public LeaderInfo leaderInfo()
    {
        return leaderInfo;
    }

    /**
     * The merge rules for Leader info are simple: the LeaderInfo with the greater term wins.
     *
     * If the leaderInfos being compared have the same term, then the info with a non-null memberId wins
     * unless, the info with a null memberId is also stepping down, in which case it wins.
     *
     * I.e. Higher term > Is stepping down > Non-null member > Null member not stepping down
     *
     * In the event of two non null memberIds in the same term (this should be impossible!) the
     * greater memberId (via UUID#compareTo) is chosen.
     *
     *                                                That
     *        +----------------------+----------+------------+---------------+----------------------+
     *        |      Conditions      | term     | isStepDown | nonNullMember | nullMemberNoStepDown |
     *        +----------------------+----------+------------+---------------+----------------------+
     *        | term                 | greater  | this       | this          | this                 |
     *  This  | isStepDown           | that     | that       | this          | this                 |
     *        | nonNullMember        | that     | that       | greater       | this                 |
     *        | nullMemberNoStepDown | that     | that       | that          | that                 |
     *        +----------------------+----------+------------+---------------+----------------------+
     *
     * @param that the leaderInfo to "merge" with
     * @return the leaderInfo which "won" the merge operation
     */
    @Override
    public ReplicatedLeaderInfo mergeData( ReplicatedLeaderInfo that )
    {
        if ( Objects.equals( that, this ) )
        {
            return that;
        }

        return sortByTerm( that, this )
                .or( () -> sortBySteppingDown( that, this ) )
                .or( () -> sortByMemberId( that, this ) )
                .orElse( that );
    }

    private static Optional<ReplicatedLeaderInfo> sortByTerm( ReplicatedLeaderInfo left, ReplicatedLeaderInfo right )
    {
        var leftGreater = left.leaderInfo.term() > right.leaderInfo.term();
        var rightGreater = right.leaderInfo.term() > left.leaderInfo.term();

        return leftGreater ? Optional.of( left ) :
               rightGreater ? Optional.of( right ) :
               Optional.empty();
    }

    private static Optional<ReplicatedLeaderInfo> sortBySteppingDown( ReplicatedLeaderInfo left, ReplicatedLeaderInfo right )
    {
        return left.leaderInfo.isSteppingDown() ? Optional.of( left ) :
               right.leaderInfo.isSteppingDown() ? Optional.of( right ) :
               Optional.empty();
    }

    private static Optional<ReplicatedLeaderInfo> sortByMemberId( ReplicatedLeaderInfo left, ReplicatedLeaderInfo right )
    {
        var uuidComp = Comparator.nullsFirst( Comparator.comparing( RaftMemberId::uuid ) );
        var comp = uuidComp.compare( left.leaderInfo.memberId(), right.leaderInfo.memberId() );
        var leftIsGreater = comp > 0;
        var rightIsGreater = comp < 0;

        return leftIsGreater ? Optional.of( left ) :
               rightIsGreater ? Optional.of( right ) :
               Optional.empty();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ReplicatedLeaderInfo that = (ReplicatedLeaderInfo) o;
        return Objects.equals( leaderInfo, that.leaderInfo );
    }

    @Override
    public String toString()
    {
        return "ReplicatedLeaderInfo{" +
               "leaderInfo=" + leaderInfo +
               '}';
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( leaderInfo );
    }
}

