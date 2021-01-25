/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import java.util.UUID;

import org.neo4j.dbms.identity.AbstractIdentityModule;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.logging.LogProvider;

/**
 * For the time being read replicas cannot have a persistent ServerId because RR rejoining discovery show all kind of funky behaviours, but mostly
 * problems. This class is to replace as soon a solution for the discovery is found
 */
public class ReadReplicaIdentityModule extends AbstractIdentityModule
{
    private final ServerId serverId;

    public static ReadReplicaIdentityModule create( LogProvider logProvider )
    {
        return new ReadReplicaIdentityModule( logProvider );
    }

    public ReadReplicaIdentityModule( LogProvider logProvider )
    {
        this.serverId = new ServerId( UUID.randomUUID() );
        var log = logProvider.getLog( getClass() );
        log.info( "Creating transient ServerID for read replica: %s", serverId );
    }

    @Override
    public ServerId serverId()
    {
        return serverId;
    }
}
