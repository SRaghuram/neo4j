/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.helper.Workload;

abstract class RepeatOnRandomCore extends Workload implements WorkOnMember
{
    private final Cluster cluster;

    RepeatOnRandomCore( Control control, Resources resources )
    {
        super( control );
        this.cluster = resources.cluster();
    }

    @Override
    protected final void doWork() throws Exception
    {
        doWorkOnMember( cluster.randomCoreMember( true ).orElseThrow( IllegalStateException::new ) );
    }

    @Override
    public abstract void doWorkOnMember( ClusterMember core ) throws Exception;
}
