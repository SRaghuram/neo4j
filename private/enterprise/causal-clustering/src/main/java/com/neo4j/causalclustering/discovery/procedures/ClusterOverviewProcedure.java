/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.internal.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.fromList;

/**
 * Overview procedure with added support for server groups.
 */
public class ClusterOverviewProcedure extends CallableProcedure.BasicProcedure
{
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "cluster"};
    public static final String PROCEDURE_NAME = "overview";

    private final TopologyService topologyService;

    public ClusterOverviewProcedure( TopologyService topologyService )
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
        var databases = coreInfo.startedDatabaseIds()
                .stream()
                .collect( toMap( identity(), databaseId -> topologyService.lookupRole( databaseId, memberId ) ) );

        return new ResultRow( memberId.getUuid(), coreInfo.connectors(), databases, coreInfo.groups() );
    }

    private static ResultRow buildResultRowForReadReplica( MemberId memberId, ReadReplicaInfo readReplicaInfo )
    {
        var databases = readReplicaInfo.startedDatabaseIds()
                .stream()
                .collect( toMap( identity(), ignore -> RoleInfo.READ_REPLICA ) );

        return new ResultRow( memberId.getUuid(), readReplicaInfo.connectors(), databases, readReplicaInfo.groups() );
    }

    private static AnyValue[] formatResultRow( ResultRow row )
    {
        return new AnyValue[]{
                stringValue( row.memberId.toString() ),
                formatAddresses( row ),
                formatDatabases( row ),
                formatGroups( row ),
        };
    }

    private static AnyValue formatAddresses( ResultRow row )
    {
        List<AnyValue> stringValues = row.addresses.uriList()
                .stream()
                .map( URI::toString )
                .map( Values::stringValue )
                .collect( toList() );

        return fromList( stringValues );
    }

    private static AnyValue formatDatabases( ResultRow row )
    {
        var builder = new MapValueBuilder();
        for ( var entry : row.databases.entrySet() )
        {
            var databaseId = entry.getKey();
            var roleString = entry.getValue().toString();
            builder.add( databaseId.name(), stringValue( roleString ) );
        }
        return builder.build();
    }

    private static AnyValue formatGroups( ResultRow row )
    {
        List<AnyValue> stringValues = row.groups.stream()
                .sorted()
                .map( Values::stringValue )
                .collect( toList() );

        return fromList( stringValues );
    }

    static class ResultRow implements Comparable<ResultRow>
    {
        final UUID memberId;
        final ClientConnectorAddresses addresses;
        final Map<DatabaseId,RoleInfo> databases;
        final Set<String> groups;

        ResultRow( UUID memberId, ClientConnectorAddresses addresses, Map<DatabaseId,RoleInfo> databases, Set<String> groups )
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
