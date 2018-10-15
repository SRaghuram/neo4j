/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.directory;

import akka.cluster.ddata.AbstractReplicatedData;
import akka.cluster.ddata.ReplicatedData;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;

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

    @Override
    public ReplicatedLeaderInfo mergeData( ReplicatedLeaderInfo that )
    {
        if ( that.leaderInfo.term() > leaderInfo.term() )
        {
            return that;
        }
        else if ( that.leaderInfo.term() < leaderInfo.term() || leaderInfo.isSteppingDown() )
        {
            return this;
        }

        return that;
    }
}

