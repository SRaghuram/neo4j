/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.index.schema;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataMatching;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.index.schema.config.CrsConfig;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.TestLabels.LABEL_ONE;

@ClusterExtension
class IndexConfigCausalClusterIT
{
    @Inject
    private static ClusterFactory clusterFactory;

    private static Cluster cluster;
    private final String prop = "prop";
    private IndexDescriptor index;
    private static final Setting<List<Double>> configuredSetting = CrsConfig.group( CoordinateReferenceSystem.Cartesian ).max;

    @BeforeAll
    static void startCluster() throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withInstanceCoreParam( configuredSetting, i -> i + ", " + i );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void indexConfigForSpatialIndexMustPropagateInCluster() throws Exception
    {
        // Make sure spatial settings are indeed unique per core member.
        Set<Integer> existingSettingsValues = new HashSet<>();
        for ( CoreClusterMember coreMember : cluster.coreMembers() )
        {
            final int maxXAsInt = coreMember.config().get( configuredSetting ).get( 0 ).intValue();
            assertTrue( existingSettingsValues.add( maxXAsInt ),
                    "expected all core members to have different settings values" );
        }

        // Create index and make sure it propagates to all cores.
        cluster.coreTx( ( db, tx ) ->
        {
            IndexDefinitionImpl indexDefinition = (IndexDefinitionImpl) tx.schema().indexFor( LABEL_ONE ).on( prop ).create();
            index = indexDefinition.getIndexReference();
            tx.commit();
        } );

        DataMatching.dataMatchesEventually( cluster.awaitLeader(), cluster.coreMembers() );

        // Validate index has the same config on all cores even though they are configured with different settings.
        Map<String,Value> first = null;
        for ( CoreClusterMember coreMember : cluster.coreMembers() )
        {
            if ( first == null )
            {
                first = getIndexConfig( coreMember.defaultDatabase() );
                assertFalse( first.isEmpty() );
            }
            else
            {
                assertEquals( first, getIndexConfig( coreMember.defaultDatabase() ) );
            }
        }
    }

    private Map<String,Value> getIndexConfig( GraphDatabaseFacade db )
    {
        Map<String,Value> indexConfig;
        try ( Transaction tx = db.beginTx() )
        {
            IndexingService indexingService = getIndexingService( db );
            try
            {
                IndexProxy indexProxy = indexingService.getIndexProxy( index );
                indexConfig = indexProxy.indexConfig();
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new RuntimeException( e );
            }
            tx.commit();
        }
        return indexConfig;
    }

    private static IndexingService getIndexingService( GraphDatabaseFacade db )
    {
        return db.getDependencyResolver().resolveDependency( IndexingService.class );
    }
}
