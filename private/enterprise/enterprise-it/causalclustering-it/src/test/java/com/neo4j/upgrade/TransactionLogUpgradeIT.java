/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.upgrade;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.function.Consumer;

import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@ResourceLock( Resources.SYSTEM_OUT )
class TransactionLogUpgradeIT
{

    @Inject
    private static ClusterFactory clusterFactory;

    private static Cluster cluster;
    private static GraphDatabaseService systemDb;

    @BeforeAll
    static void beforeAll() throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                                                   .withNumberOfCoreMembers( 2 )
                                                   .withNumberOfReadReplicas( 1 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        var systemLeader = cluster.awaitLeader( SYSTEM_DATABASE_NAME );
        systemDb = systemLeader.managementService().database( SYSTEM_DATABASE_NAME );
    }

    @AfterAll
    static void afterAll()
    {
        cluster.shutdown();
    }

    @Test
    void testBasicVersionLifecycle()
    {
        // the system DB will be initialised with the default version for this binary
        assertRuntimeVersion( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );

        // BTW this should never be manipulated directly outside tests
        setRuntimeVersion( DbmsRuntimeVersion.V4_1 );

        assertRuntimeVersion( DbmsRuntimeVersion.V4_1 );

        systemDb.executeTransactionally( "CALL dbms.upgrade()" );

        assertRuntimeVersion( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );
    }

    private void setRuntimeVersion( DbmsRuntimeVersion runtimeVersion )
    {
        try ( var tx = systemDb.beginTx() )
        {
            tx.findNodes( VERSION_LABEL )
              .stream()
              .forEach( dbmsRuntimeNode -> dbmsRuntimeNode.setProperty( DBMS_RUNTIME_COMPONENT, runtimeVersion.getVersion() ) );

            tx.commit();
        }

        doOnAllDbmsRuntimeRepositories( dbmsRuntimeRepository -> dbmsRuntimeRepository.setVersion( runtimeVersion ) );
    }

    private void assertRuntimeVersion( DbmsRuntimeVersion expectedRuntimeVersion )
    {
        doOnAllDbmsRuntimeRepositories(
                dbmsRuntimeRepository -> assertEventually( dbmsRuntimeRepository::getVersion, equalityCondition( expectedRuntimeVersion ), 5, SECONDS )
        );
    }

    private void doOnAllDbmsRuntimeRepositories( Consumer<DbmsRuntimeRepository> repositoryConsumer )
    {
        cluster.allMembers().stream()
               .map( clusterMember -> clusterMember.resolveDependency( "system", DbmsRuntimeRepository.class ) )
               .forEach( repositoryConsumer );
    }
}
