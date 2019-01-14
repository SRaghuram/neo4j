/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.election;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * If an instance is considered failed, demote it from all its roles in the cluster.
 * If an instance comes back, ensure that all roles are elected.
 */
public class HeartbeatReelectionListener
    implements HeartbeatListener
{
    private final Election election;
    private final Log log;

    public HeartbeatReelectionListener( Election election, LogProvider logProvider )
    {
        this.election = election;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void failed( InstanceId server )
    {
        // Suggest reelection for all roles of this node
        log.warn( " instance " + server + " is being demoted since it failed" );
        election.demote( server );
    }

    @Override
    public void alive( InstanceId server )
    {
        election.performRoleElections();
    }
}
