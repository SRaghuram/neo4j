/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readonly;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.switchLeaderTo;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_topology_refresh;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
@TestInstance( PER_METHOD )
public class ReadOnlyCoreIT
{
    @Inject
    ClusterFactory clusterFactory;

    Cluster cluster;

    @Test
    public void whenAllCoresAreReadOnlyNoOneShouldHaveAppliedTx() throws Exception
    {
        final ClusterConfig config = getClusterConfig( Set.of(), true, true );
        cluster = clusterFactory.createCluster( config );
        cluster.start();
        cluster.awaitLeader();

        //when
        final var property = "property";
        final var label = "test";
        assertThrows( RuntimeException.class, () ->
                cluster.coreTx( DEFAULT_DATABASE_NAME, ( db, tx ) ->
                {
                    tx.execute( "CREATE (:test {" + property + ":\"" + label + "\"})" );
                    tx.commit();
                }, 5, TimeUnit.SECONDS ) );

        //then membersWithNotAppliedTx are 3
        assertEventually( () -> countMembers( cluster, facade ->
                          {
                              final var propertyExists = dataExists( facade, tx ->
                              {
                                  final var iterator = tx.getAllNodes().iterator();
                                  assertThat( iterator.next().getPropertyKeys().iterator().next() ).isEqualTo( property );
                                  iterator.close();
                              } );
                              final var labelExists = dataExists( facade, tx ->
                                      assertThat( tx.getAllLabels().iterator().next().name() ).isEqualTo( label ) );
                              return !propertyExists && !labelExists;
                          } ),
                          members -> members == 3, 1, TimeUnit.MINUTES );
    }

    @Test
    public void shouldApplyTxToAllMembersIfOneOfTheFollowersIsReadOnly() throws Exception
    {
        final var config = getClusterConfig( Set.of(), true, false );

        cluster = clusterFactory.createCluster( config );
        cluster.start();
        cluster.awaitLeader();
        var newLeader = cluster.getCoreMemberByIndex( 1 ); // We configure member 0 to be read only
        switchLeaderTo( cluster, newLeader );

        //when
        final var property = "property";
        final var label = "test";
        cluster.coreTx( ( db, tx ) ->
                        {
                            tx.execute( "CREATE (:test {" + property + ":\"" + label + "\"})" );
                            tx.commit();
                        } );

        //then membersWithAppliedTx are 3
        assertEventually( () -> countMembers( cluster, facade ->
                          {
                              final var propertyExists = dataExists( facade, tx ->
                              {
                                  final var iterator = tx.getAllNodes().iterator();
                                  assertThat( iterator.next().getPropertyKeys().iterator().next() ).isEqualTo( property );
                                  iterator.close();
                              } );
                              final var labelExists = dataExists( facade, tx ->
                                      assertThat( tx.getAllLabels().iterator().next().name() ).isEqualTo( label ) );
                              return propertyExists && labelExists;
                          } ),
                          members -> members == 3, 1, TimeUnit.MINUTES );
    }

    @Test
    public void whenLeaderIsInReadOnlyModeNoOneShouldApplyTx() throws Exception
    {
        final var config = getClusterConfig( Set.of(), true, false );

        cluster = clusterFactory.createCluster( config );
        cluster.start();
        cluster.awaitLeader();

        //when
        final var property = "property";
        final var label = "test";
        assertThrows( RuntimeException.class, () ->
                cluster.coreTx( DEFAULT_DATABASE_NAME, ( db, tx ) ->
                {
                    tx.execute( "CREATE (:test {" + property + ":\"" + label + "\"})" );
                    tx.commit();
                }, 5, TimeUnit.SECONDS ) );

        //then membersWithNotAppliedTx are 3
        assertEventually( () -> countMembers( cluster, facade ->
                          {
                              final var propertyExists = dataExists( facade, tx ->
                              {
                                  final var iterator = tx.getAllNodes().iterator();
                                  assertThat( iterator.next().getPropertyKeys().iterator().next() ).isEqualTo( property );
                                  iterator.close();
                              } );
                              final var labelExists = dataExists( facade, tx ->
                                      assertThat( tx.getAllLabels().iterator().next().name() ).isEqualTo( label ) );
                              return !propertyExists && !labelExists;
                          } ),
                          members -> members == 3, 1, TimeUnit.MINUTES );
    }

    @Test
    public void shouldWriteToDatabaseThatIsAvailableForWrite() throws Exception
    {
        //given
        final var fooDB = "foo1";
        final var config = getClusterConfig( Set.of( fooDB ), false, false ); //fooDB is readOnly
        cluster = clusterFactory.createCluster( config );
        cluster.start();
        cluster.awaitLeader();

        //when
        final var property = "property";
        final var label = "test";

        cluster.coreTx( DEFAULT_DATABASE_NAME, ( db, tx ) ->
        {
            tx.execute( "CREATE (:test {" + property + ":\"" + label + "\"})" );
            tx.commit();
        } );

        //membersWithAppliedTx are 3
        assertEventually( () -> countMembers( cluster, facade ->
                          {
                              final var propertyExists = dataExists( facade, tx ->
                              {
                                  final var iterator = tx.getAllNodes().iterator();
                                  assertThat( iterator.next().getPropertyKeys().iterator().next() ).isEqualTo( property );
                                  iterator.close();
                              } );
                              final var labelExists = dataExists( facade, tx ->
                                      assertThat( tx.getAllLabels().iterator().next().name() ).isEqualTo( label ) );
                              return propertyExists && labelExists;
                          } ),
                          members -> members == 3, 1, TimeUnit.MINUTES );
    }

    @Test
    void shouldNotBeAbleToWriteOnReadOnlyDatabase() throws Exception
    {
        //given
        final var fooDB = "foo1";
        final var config = getClusterConfig( Set.of( fooDB ), false, true ); //fooDB is readOnly
        cluster = clusterFactory.createCluster( config );
        cluster.start();
        cluster.awaitLeader();

        //create fooDB
        createDatabase( fooDB, cluster );
        assertDatabaseEventuallyStarted( fooDB, cluster );

        //should not succeed to execute transaction
        final var property = "property";
        final var label = "test";

        assertThrows( RuntimeException.class, () ->
                cluster.coreTx( fooDB, ( db, tx ) ->
                {
                    tx.execute( "CREATE (:test {" + property + ":\"" + label + "\"})" );
                    tx.commit();
                }, 1, TimeUnit.MINUTES ) );
    }

    private long countMembers( Cluster cluster, Predicate<GraphDatabaseFacade> countFunction )
    {
        return cluster.coreMembers()
                      .stream()
                      .map( ClusterMember::defaultDatabase )
                      .filter( countFunction )
                      .count();
    }

    private boolean dataExists( GraphDatabaseFacade facade, Consumer<Transaction> validationFunc )
    {
        try ( var tx = facade.beginTx() )
        {
            try
            {
                validationFunc.accept( tx );
                return true;
            }
            catch ( Exception ignored )
            {
            }
        }
        return false;
    }

    private ClusterConfig getClusterConfig( Set<String> readOnlyDatabases, boolean globalReadOnly, boolean all )
    {
        var config = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( cluster_topology_refresh, "5s" );

        var dbsStr = String.join( ",", readOnlyDatabases );

        if ( all )
        {
            return config.withSharedCoreParam( GraphDatabaseSettings.read_only_databases, dbsStr )
                         .withSharedCoreParam( GraphDatabaseSettings.read_only_database_default, String.valueOf( globalReadOnly ) );
        }

        return config.withInstanceCoreParam( GraphDatabaseSettings.read_only_databases, id -> id == 0 ? dbsStr : "" )
                     .withInstanceCoreParam( GraphDatabaseSettings.read_only_database_default, id -> id == 0 ? String.valueOf( globalReadOnly ) : "false" );
    }
}
