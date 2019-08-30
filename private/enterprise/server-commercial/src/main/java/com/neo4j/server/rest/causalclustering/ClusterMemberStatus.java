/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.identity.MemberId;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.repr.OutputFormat;

import static com.neo4j.server.rest.causalclustering.CausalClusteringService.relativeDatabaseClusterPath;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.status;

abstract class ClusterMemberStatus implements CausalClusteringStatus
{
    protected final OutputFormat output;
    protected final GraphDatabaseAPI db;

    ClusterMemberStatus( OutputFormat output, GraphDatabaseAPI db )
    {
        this.output = output;
        this.db = db;
    }

    @Override
    public final Response discover()
    {
        return output.ok( new CausalClusteringDiscovery( relativeDatabaseClusterPath( db.databaseName() ) ) );
    }

    Response statusResponse( long lastAppliedRaftIndex, boolean isParticipatingInRaftGroup, Collection<MemberId> votingMembers, boolean isHealthy,
            MemberId memberId, MemberId leader, Duration millisSinceLastLeaderMessage, Double raftCommandsPerSecond, boolean isCore )
    {
        String jsonObject;
        ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            jsonObject = objectMapper.writeValueAsString(
                    new ClusterStatusResponse( lastAppliedRaftIndex, isParticipatingInRaftGroup, votingMembers, isHealthy, memberId, leader,
                            millisSinceLastLeaderMessage, raftCommandsPerSecond, isCore ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return status( OK ).type( MediaType.APPLICATION_JSON ).entity( jsonObject ).build();
    }

    Response positiveResponse()
    {
        return plainTextResponse( OK, "true" );
    }

    Response negativeResponse()
    {
        return plainTextResponse( NOT_FOUND, "false" );
    }

    private Response plainTextResponse( Response.Status status, String entityBody )
    {
        return status( status ).type( TEXT_PLAIN_TYPE ).entity( entityBody ).build();
    }
}
