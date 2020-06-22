/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.helper.Workload;

import java.util.Set;
import java.util.concurrent.TimeUnit;

abstract class RepeatOnLeader extends Workload implements WorkOnMember
{
    private final Cluster cluster;
    protected final String databaseName;

    RepeatOnLeader( Control control, Resources resources, String databaseName )
    {
        super( control );
        this.cluster = resources.cluster();
        this.databaseName = databaseName;
    }

    @Override
    protected final void doWork() throws Exception
    {
        cluster.awaitAllCoresJoinedAllRaftGroups( Set.of( databaseName ), 3, TimeUnit.MINUTES );
        doWorkOnMember( cluster.awaitLeader( databaseName ) );
    }

    @Override
    public void doWorkOnMember( ClusterMember core ) throws Exception
    {
        doWorkOnLeader( core );
    }

    public abstract void doWorkOnLeader( ClusterMember leader ) throws Exception;
}
