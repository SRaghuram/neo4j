/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.TimeoutException;

import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@ClusterExtension
class StandaloneClusterIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withStandalone()
            .withNumberOfReadReplicas( 2 );

    @BeforeEach
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void replicatesToReadReplicas() throws TimeoutException
    {
        cluster.getStandaloneMember().database( DEFAULT_DATABASE_NAME ).executeTransactionally( "CREATE (n:some {id:0})" );
        dataMatchesEventually( cluster.getStandaloneMember(), cluster.readReplicas() );
    }

    @Test
    void topologyInfoIsCorrect()
    {
        var serverId = cluster.getStandaloneMember().serverId();
        var topologyServiceOnStandalone = cluster.getStandaloneMember().resolveDependency( SYSTEM_DATABASE_NAME, TopologyService.class );
        var topologyServiceOnOneReplica = cluster.getStandaloneMember().resolveDependency( SYSTEM_DATABASE_NAME, TopologyService.class );

        Assertions.assertEquals( 1, topologyServiceOnStandalone.allCoreServers().size() );
        Assertions.assertEquals( 1, topologyServiceOnOneReplica.allCoreServers().size() );
        Assertions.assertEquals( 2, topologyServiceOnStandalone.allReadReplicas().size() );
        Assertions.assertEquals( 2, topologyServiceOnOneReplica.allReadReplicas().size() );
        Assertions.assertEquals( RoleInfo.LEADER, topologyServiceOnStandalone.lookupRole( NAMED_SYSTEM_DATABASE_ID, serverId ),
                "Standalone should think itself is leader" );
        Assertions.assertEquals( RoleInfo.LEADER, topologyServiceOnOneReplica.lookupRole( NAMED_SYSTEM_DATABASE_ID, serverId ),
                "Read replica should think standalone is leader" );
    }
}
