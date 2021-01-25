/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.ClusterMember;

class StartStopRandomMember extends RepeatOnRandomMember
{
    private final StartStopMember startStop;

    StartStopRandomMember( Control control, Resources resources )
    {
        super( control, resources );
        this.startStop = new StartStopMember( resources );
    }

    @Override
    public void doWorkOnMember( ClusterMember member ) throws Exception
    {
        startStop.doWorkOnMember( member );
    }
}
