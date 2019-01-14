/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.stresstests;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.common.ClusterMember;
import org.neo4j.helper.Workload;

abstract class RepeatOnRandomMember extends Workload implements WorkOnMember
{
    private final Cluster<?> cluster;

    RepeatOnRandomMember( Control control, Resources resources )
    {
        super( control );
        this.cluster = resources.cluster();
    }

    @Override
    protected final void doWork() throws Exception
    {
        doWorkOnMember( cluster.randomMember( true ).orElseThrow( IllegalStateException::new ) );
    }

    @Override
    public abstract void doWorkOnMember( ClusterMember member ) throws Exception;
}
