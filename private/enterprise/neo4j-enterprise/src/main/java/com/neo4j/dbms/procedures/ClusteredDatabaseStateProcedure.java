/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.OperatorState;
import org.neo4j.dbms.procedures.DatabaseStateProcedure;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.values.AnyValue;

public class ClusteredDatabaseStateProcedure extends DatabaseStateProcedure
{
    private final TopologyService topologyService;
    private final MemberId myself;
    private final DatabaseStateService stateService;

    public ClusteredDatabaseStateProcedure( DatabaseIdRepository idRepository, TopologyService topologyService, DatabaseStateService stateService )
    {
        super( idRepository );
        this.topologyService = topologyService;
        this.myself = topologyService.memberId();
        this.stateService = stateService;
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

    private Stream<AnyValue[]> resultRowsForExistingMembers( Map<MemberId,? extends DiscoveryServerInfo> serverInfos,
            MemberId memberId, DatabaseId databaseId )
    {
        return Stream.ofNullable( serverInfos.get( memberId ) )
                .map( discoveryInfo -> resultRowFactory( databaseId, topologyService, memberId, discoveryInfo ) );
    }

    private AnyValue[] resultRowFactory( DatabaseId databaseId, TopologyService topologyService, MemberId memberId,
            DiscoveryServerInfo serverInfo )
    {
        var role = topologyService.lookupRole( databaseId, memberId );
        var roleString = role.name().toLowerCase();
        var address = serverInfo.boltAddress().toString();
        var isMe = Objects.equals( memberId, myself );
        var stateService = isMe ? this.stateService : new RemoteDatabaseStateService( id -> topologyService.lookupDatabaseState( id, memberId ) );

        return resultRowFactory( databaseId, roleString, address, stateService );
    }

    private static class RemoteDatabaseStateService implements DatabaseStateService
    {
        private final Function<DatabaseId,DatabaseState> stateLookup;

        RemoteDatabaseStateService( Function<DatabaseId,DatabaseState> stateLookup )
        {
            this.stateLookup = stateLookup;
        }

        @Override
        public OperatorState stateOfDatabase( DatabaseId databaseId )
        {
            return stateLookup.apply( databaseId ).operatorState();
        }

        @Override
        public Optional<Throwable> causeOfFailure( DatabaseId databaseId )
        {
            return stateLookup.apply( databaseId ).failure();
        }
    }
}

