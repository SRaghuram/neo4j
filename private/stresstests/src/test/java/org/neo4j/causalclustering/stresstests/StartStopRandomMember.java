/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.stresstests;

import org.neo4j.causalclustering.discovery.ClusterMember;

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
