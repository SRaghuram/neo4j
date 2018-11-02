/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.election;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * When an instance leaves a cluster, demote it from all its current roles.
 */
public class ClusterLeaveReelectionListener
        extends ClusterListener.Adapter
{
    private final Election election;
    private final Log log;

    public ClusterLeaveReelectionListener( Election election, LogProvider logProvider )
    {
        this.election = election;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void leftCluster( InstanceId instanceId, URI member )
    {
        String name = instanceId.instanceNameFromURI( member );
        log.warn( "Demoting member " + name + " because it left the cluster" );
        // Suggest reelection for all roles of this node
        election.demote( instanceId );
    }
}
