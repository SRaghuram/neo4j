/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.configuration.ServerGroupName;

import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValueBuilder;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.internal.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.values.storable.Values.utf8Value;

/**
 * Overview procedure with added support for server groups.
 */
public class ClusterOverviewProcedure extends CallableProcedure.BasicProcedure
{
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "cluster"};
    public static final String PROCEDURE_NAME = "overview";

    private final TopologyService topologyService;
    private final DatabaseIdRepository databaseIdRepository;

    public ClusterOverviewProcedure( TopologyService topologyService, DatabaseIdRepository databaseIdRepository )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .out( "id", Neo4jTypes.NTString )
                .out( "addresses", Neo4jTypes.NTList( Neo4jTypes.NTString ) )
                .out( "databases", Neo4jTypes.NTMap )
                .out( "groups", Neo4jTypes.NTList( Neo4jTypes.NTString ) )
                .description( "Overview of all currently accessible cluster members, their databases and roles." )
                .systemProcedure()
                .build() );
        this.topologyService = topologyService;
        this.databaseIdRepository = databaseIdRepository;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker )
    {
        var resultRows = new ArrayList<ResultRow>();

        for ( var entry : topologyService.allCoreServers().entrySet() )
        {
            var row = buildResultRowForCore( entry.getKey(), entry.getValue() );
            resultRows.add( row );
        }

        for ( var entry : topologyService.allReadReplicas().entrySet() )
        {
            var row = buildResultRowForReadReplica( entry.getKey(), entry.getValue() );
            resultRows.add( row );
        }

        var resultStream = resultRows.stream()
                .sorted()
                .map( ClusterOverviewProcedure::formatResultRow );

        return asRawIterator( resultStream );
    }

    private ResultRow buildResultRowForCore( MemberId memberId, CoreServerInfo coreInfo )
    {
        return buildResultRow( memberId, coreInfo, databaseId -> topologyService.lookupRole( databaseId, memberId ) );
    }

    private ResultRow buildResultRowForReadReplica( MemberId memberId, ReadReplicaInfo readReplicaInfo )
    {
        return buildResultRow( memberId, readReplicaInfo, ignore -> RoleInfo.READ_REPLICA );
    }

    private ResultRow buildResultRow( MemberId memberId, DiscoveryServerInfo serverInfo, Function<NamedDatabaseId,RoleInfo> result )
    {
        var databases = serverInfo.startedDatabaseIds()
                .stream()
                .flatMap( databaseId -> databaseIdRepository.getById( databaseId ).stream() )
                .collect( toMap( identity(), result ) );

        return new ResultRow( memberId.getUuid(), serverInfo.connectors(), databases, serverInfo.groups() );
    }

    private static AnyValue[] formatResultRow( ResultRow row )
    {
        return new AnyValue[]{
                utf8Value( row.memberId.toString() ),
                formatAddresses( row ),
                formatDatabases( row ),
                formatGroups( row ),
        };
    }

    private static AnyValue formatAddresses( ResultRow row )
    {
        return row.addresses.publicUriList()
                .stream()
                .map( URI::toString )
                .map( Values::utf8Value )
                .collect( ListValueBuilder.collector() );
    }

    private static AnyValue formatDatabases( ResultRow row )
    {
        var builder = new MapValueBuilder();
        for ( var entry : row.databases.entrySet() )
        {
            var databaseId = entry.getKey();
            var roleString = entry.getValue().toString();
            builder.add( databaseId.name(), utf8Value( roleString ) );
        }
        return builder.build();
    }

    private static AnyValue formatGroups( ResultRow row )
    {
        return row.groups.stream()
                .sorted()
                .map( ServerGroupName::getRaw )
                .map( Values::utf8Value )
                .collect( ListValueBuilder.collector() );
    }

    static class ResultRow implements Comparable<ResultRow>
    {
        final UUID memberId;
        final ConnectorAddresses addresses;
        final Map<NamedDatabaseId,RoleInfo> databases;
        final Set<ServerGroupName> groups;

        ResultRow( UUID memberId, ConnectorAddresses addresses, Map<NamedDatabaseId,RoleInfo> databases, Set<ServerGroupName> groups )
        {
            this.memberId = memberId;
            this.addresses = addresses;
            this.databases = databases;
            this.groups = groups;
        }

        @Override
        public int compareTo( ResultRow other )
        {
            return memberId.compareTo( other.memberId );
        }
    }
}
