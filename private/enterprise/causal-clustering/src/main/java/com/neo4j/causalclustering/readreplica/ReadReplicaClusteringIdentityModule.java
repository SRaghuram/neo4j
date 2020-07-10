/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.UUID;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;

/**
 * For the time being read replicas cannot have a persistent ServerId/MemberId because RR rejoining discovery show all kind of funky behaviours, but mostly
 * problems. This class is to replace as soon a solution for the discovery is found
 */
@Deprecated
public class ReadReplicaClusteringIdentityModule implements ClusteringIdentityModule
{
    private final ServerId serverId;
    private final MemberId memberId;

    public static ReadReplicaClusteringIdentityModule create( LogProvider logProvider )
    {
        return new ReadReplicaClusteringIdentityModule( logProvider );
    }

    private ReadReplicaClusteringIdentityModule( LogProvider logProvider )
    {
        var uuid = UUID.randomUUID();
        serverId = new ServerId( uuid );
        memberId = new MemberId( uuid );
        var log = logProvider.getLog( getClass() );
        log.info( "Creating transient ServerID/MemberId for read replica: %s/%s (%s)", serverId, memberId, uuid );
    }

    @Override
    public ServerId myself()
    {
        return serverId;
    }

    @Override
    public MemberId memberId()
    {
        return memberId;
    }

    @Override
    public MemberId memberId( NamedDatabaseId namedDatabaseId )
    {
        return memberId;
    }
}
