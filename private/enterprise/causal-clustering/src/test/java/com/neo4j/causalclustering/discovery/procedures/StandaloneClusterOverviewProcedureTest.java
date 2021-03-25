/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.discovery.procedures.TopologyBasedClusterOverviewProcedureTest.IsRecord;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.values.AnyValue;

import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static com.neo4j.causalclustering.discovery.procedures.TopologyBasedClusterOverviewProcedureTest.databaseIds;
import static com.neo4j.causalclustering.discovery.procedures.TopologyBasedClusterOverviewProcedureTest.databasesWithRole;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;

class StandaloneClusterOverviewProcedureTest
{
    private ServerId myself = new ServerId( new UUID( 1, 0 ) );
    private Config config = Config.newBuilder()
            .set( BoltConnector.advertised_address, new SocketAddress( 5000 ) )
            .set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "oneAndOnly" ) )
            .build();
    private StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();

    @Test
    void shouldHaveCorrectSignature()
    {
        ClusterOverviewProcedure procedure = new StandaloneClusterOverviewProcedure( myself, config, databaseManager );

        ProcedureSignature signature = procedure.signature();

        assertEquals( "dbms.cluster.overview", signature.name().toString() );
        assertEquals( List.of(), signature.inputSignature() );
        assertTrue( signature.systemProcedure() );
        assertEquals(
                List.of( outputField( "id", Neo4jTypes.NTString ),
                        outputField( "addresses", Neo4jTypes.NTList( Neo4jTypes.NTString ) ),
                        outputField( "databases", Neo4jTypes.NTMap ),
                        outputField( "groups", Neo4jTypes.NTList( Neo4jTypes.NTString ) ) ),
                signature.outputSignature() );
    }

    @Test
    void shouldProvideOverviewOfItselfOnly() throws Exception
    {
        // given
        var databases = databaseIds( "customers", "orders", "system" );
        databases.forEach( databaseId -> databaseManager.registerDatabase( databaseId, null ) );

        var procedure = new StandaloneClusterOverviewProcedure( myself, config, databaseManager );

        // when
        final RawIterator<AnyValue[],ProcedureException> members = procedure.apply( null, new AnyValue[0], null );

        // then
        assertThat( members.next(), new IsRecord( myself, 5000, databasesWithRole( databases, LEADER ), Set.of( "oneAndOnly" ) ) );

        assertFalse( members.hasNext() );
    }
}
