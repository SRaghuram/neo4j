/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.helpers.DataCreator;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.neo4j.helpers.collection.Pair;

import static com.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.compression_implementations;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;

@RunWith( Parameterized.class )
public class ClusterCompressionIT
{
    @Parameterized.Parameter
    public Protocol.ModifierProtocol modifierProtocol;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Protocol.ModifierProtocol> params()
    {
        return Arrays.asList( ModifierProtocols.values() );
    }

    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule()
                    .withNumberOfCoreMembers( 3 )
                    .withNumberOfReadReplicas( 3 )
                    .withTimeout( 1000, SECONDS );

    @Test
    public void shouldReplicateWithCompression() throws Exception
    {
        // given
        clusterRule
                .withSharedCoreParam( compression_implementations, modifierProtocol.implementation() )
                .withSharedReadReplicaParam( compression_implementations, modifierProtocol.implementation() );

        Cluster<?> cluster = clusterRule.startCluster();

         // when
        int numberOfNodes = 10;
        CoreClusterMember leader = DataCreator.createLabelledNodesWithProperty( cluster, numberOfNodes, label( "Foo" ),
                () -> Pair.of( "foobar", format( "baz_bat%s", UUID.randomUUID() ) ) );

        // then
        assertEquals( numberOfNodes, DataCreator.countNodes( leader ) );
        dataMatchesEventually( leader, cluster.coreMembers() );
    }
}
