/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.TopologyService;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.procedures.DatabaseStateProcedure;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.AnyValue;

public class ClusteredDatabaseStateProcedure extends DatabaseStateProcedure
{
    private final TopologyService topologyService;

    public ClusteredDatabaseStateProcedure( DatabaseIdRepository idRepository, TopologyService topologyService )
    {
        super( idRepository );
        this.topologyService = topologyService;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        var databaseId = extractDatabaseId( input );

        var coreServerInfos = topologyService.allCoreServers();
        var rrServerInfos = topologyService.allReadReplicas();

        var coreStates = topologyService.allCoreStatesForDatabase( databaseId );
        var coreResultRows = coreStates.keySet().stream()
                .flatMap( member -> resultRowsForExistingMembers( coreServerInfos, member, databaseId ) );

        var rrStates = topologyService.allReadReplicaStatesForDatabase( databaseId );
        var rrResultRows = rrStates.keySet().stream()
                .flatMap( member -> resultRowsForExistingMembers( rrServerInfos, member, databaseId ) );

        return RawIterator.wrap( Stream.concat( coreResultRows, rrResultRows ).iterator() );
    }

    private Stream<AnyValue[]> resultRowsForExistingMembers( Map<ServerId,? extends DiscoveryServerInfo> serverInfos,
            ServerId serverId, NamedDatabaseId namedDatabaseId )
    {
        return Stream.ofNullable( serverInfos.get( serverId ) )
                .map( discoveryInfo -> resultRowFactory( namedDatabaseId, serverId, discoveryInfo ) );
    }

    private AnyValue[] resultRowFactory( NamedDatabaseId namedDatabaseId, ServerId serverId, DiscoveryServerInfo serverInfo )
    {
        var role = topologyService.lookupRole( namedDatabaseId, serverId );
        var roleString = role.name().toLowerCase();
        var address = serverInfo.boltAddress().toString();
        var status = topologyService.lookupDatabaseState( namedDatabaseId, serverId ).operatorState();
        var error = topologyService.lookupDatabaseState( namedDatabaseId, serverId ).failure().map( Throwable::getMessage );

        return resultRowFactory( status, error, roleString, address );
    }

}

