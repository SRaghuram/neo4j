/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.helpers.collection.Iterators.map;
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
    private final Log log;

    public ClusterOverviewProcedure( TopologyService topologyService, LogProvider logProvider )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .out( "id", Neo4jTypes.NTString )
                .out( "addresses", Neo4jTypes.NTList( Neo4jTypes.NTString ) )
                .out( "role", Neo4jTypes.NTString )
                .out( "groups", Neo4jTypes.NTList( Neo4jTypes.NTString ) )
                .out( "database", Neo4jTypes.NTString )
                .description( "Overview of all currently accessible cluster members and their roles." )
                .build() );
        this.topologyService = topologyService;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply(
            Context ctx, AnyValue[] input, ResourceTracker resourceTracker )
    {
        Map<MemberId,RoleInfo> roleMap = topologyService.allCoreRoles();
        List<ReadWriteEndPoint> endpoints = new ArrayList<>();

        CoreTopology coreTopology = topologyService.allCoreServers();
        Set<MemberId> coreMembers = coreTopology.members().keySet();

        for ( MemberId memberId : coreMembers )
        {
            Optional<CoreServerInfo> coreServerInfo = coreTopology.find( memberId );
            if ( coreServerInfo.isPresent() )
            {
                CoreServerInfo info = coreServerInfo.get();
                RoleInfo role = roleMap.getOrDefault( memberId, RoleInfo.UNKNOWN );
                endpoints.add( new ReadWriteEndPoint( info.connectors(), role, memberId.getUuid(),
                        info.groups(), info.getDatabaseIds() ) );
            }
            else
            {
                log.debug( "No Address found for " + memberId );
            }
        }

        for ( Map.Entry<MemberId,ReadReplicaInfo> readReplica : topologyService.allReadReplicas().members().entrySet() )
        {
            ReadReplicaInfo readReplicaInfo = readReplica.getValue();
            endpoints.add( new ReadWriteEndPoint( readReplicaInfo.connectors(), RoleInfo.READ_REPLICA,
                    readReplica.getKey().getUuid(), readReplicaInfo.groups(), readReplicaInfo.getDatabaseIds() ) );
        }

        endpoints.sort( comparing( o -> o.addresses().toString() ) );

        return map( endpoint -> new AnyValue[]
                        {
                                stringValue( endpoint.memberId().toString() ),
                                asListOfStringsValue( endpoint.addresses().uriList(), URI::toString ),
                                stringValue( endpoint.role().name() ),
                                asListOfStringsValue( endpoint.groups(), Function.identity() ),
                                asListOfStringsValue( endpoint.databaseIds(), DatabaseId::name ),
                        },
                asRawIterator( endpoints.iterator() ) );
    }

    private static <T> ListValue asListOfStringsValue( Collection<T> values, Function<T,String> toString )
    {
        List<AnyValue> stringValues = values.stream()
                .map( toString )
                .map( Values::stringValue )
                .collect( toList() );

        return fromList( stringValues );
    }

    static class ReadWriteEndPoint
    {
        private final ClientConnectorAddresses clientConnectorAddresses;
        private final RoleInfo role;
        private final UUID memberId;
        private final Set<String> groups;
        private final Set<DatabaseId> databaseIds;

        public ClientConnectorAddresses addresses()
        {
            return clientConnectorAddresses;
        }

        public RoleInfo role()
        {
            return role;
        }

        UUID memberId()
        {
            return memberId;
        }

        Set<String> groups()
        {
            return groups;
        }

        Set<DatabaseId> databaseIds()
        {
            return databaseIds;
        }

        ReadWriteEndPoint( ClientConnectorAddresses clientConnectorAddresses, RoleInfo role, UUID memberId, Set<String> groups, Set<DatabaseId> databaseIds )
        {
            this.clientConnectorAddresses = clientConnectorAddresses;
            this.role = role;
            this.memberId = memberId;
            this.groups = groups;
            this.databaseIds = databaseIds;
        }
    }
}
