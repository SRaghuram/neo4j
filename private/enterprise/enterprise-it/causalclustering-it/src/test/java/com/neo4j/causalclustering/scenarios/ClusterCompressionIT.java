/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocols;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.UUID;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.configuration.CausalClusteringSettings.compression_implementations;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.graphdb.Label.label;

@ClusterExtension
@TestInstance( PER_METHOD )
class ClusterCompressionIT
{
    @Inject
    private ClusterFactory clusterFactory;

    @ParameterizedTest
    @EnumSource( ModifierProtocols.class )
    void shouldReplicateWithCompression( ModifierProtocol modifierProtocol ) throws Exception
    {
        // given
        var cluster = clusterFactory.createCluster( newClusterConfig( modifierProtocol ) );
        cluster.start();

         // when
        var numberOfNodes = 10;
        var leader = DataCreator.createLabelledNodesWithProperty( cluster, numberOfNodes, label( "Foo" ),
                () -> Pair.of( "foobar", format( "baz_bat%s", UUID.randomUUID() ) ) );

        // then
        assertEquals( numberOfNodes, DataCreator.countNodes( leader ) );
        dataMatchesEventually( leader, cluster.coreMembers() );
    }

    private ClusterConfig newClusterConfig( ModifierProtocol modifierProtocol )
    {
        return clusterConfig()
                .withNumberOfCoreMembers( 2 )
                .withNumberOfReadReplicas( 1 )
                .withSharedCoreParam( compression_implementations, modifierProtocol.implementation() )
                .withSharedReadReplicaParam( compression_implementations, modifierProtocol.implementation() );
    }
}
