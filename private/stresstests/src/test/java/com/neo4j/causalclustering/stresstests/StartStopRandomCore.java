/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.ClusterMember;

class StartStopRandomCore extends RepeatOnRandomCore
{
    private final StartStopMember startStop;

    StartStopRandomCore( Control control, Resources resources )
    {
        super( control, resources );
        this.startStop = new StartStopMember( resources );
    }

    @Override
    public void doWorkOnMember( ClusterMember core ) throws Exception
    {
        startStop.doWorkOnMember( core );
    }
}
