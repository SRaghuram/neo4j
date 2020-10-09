/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.dbms.ShowDatabasesHelpers.ShowDatabasesResultRow;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@ClusterExtension
@TestInstance( PER_METHOD )
public class SetDefaultDatabaseIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void setup() throws Exception
    {
        var clusterConfig = ClusterConfig.clusterConfig()
                                         .withSharedCoreParam( GraphDatabaseSettings.auth_enabled, "true" )
                                         .withNumberOfCoreMembers( 3 )
                                         .withNumberOfReadReplicas( 0 );
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldBeNoopWhenAlreadyDefault() throws Exception
    {
        // GIVEN
        assertDefaultDatabase( "neo4j", cluster );

        cluster.systemTx( ( db, tx ) ->
        {
            // WHEN
            Result result = tx.execute( "CALL dbms.cluster.setDefaultDatabase('neo4j')" );
            Map<String,Object> row = result.next();
            // THEN
            assertThat( row.get( "result" ) ).isEqualTo( "Default database already set to neo4j, no change required" );
            tx.commit();
        } );
    }

    @Test
    void shouldChangeDatabaseWhenOldDefaultStopped() throws Exception
    {
        // GIVEN
        assertDefaultDatabase( "neo4j", cluster );
        CausalClusteringTestHelpers.createDatabase( "foo", cluster );
        CausalClusteringTestHelpers.stopDatabase( "neo4j", cluster );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( "foo", cluster );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStopped( "neo4j", cluster );

        cluster.systemTx( ( db, tx ) ->
        {
            // WHEN
            Result result = tx.execute( "CALL dbms.cluster.setDefaultDatabase('foo')" );
            Map<String,Object> row = result.next();
            // THEN
            assertThat( row.get( "result" ) ).isEqualTo( "Default database set to foo" );
            tx.commit();
        } );

        // THEN
        assertDefaultDatabase( "foo", cluster );
    }

    @Test
    void shouldChangeDatabaseWhenOldDefaultDropped() throws Exception
    {
        // GIVEN
        CausalClusteringTestHelpers.createDatabase( "foo", cluster );
        CausalClusteringTestHelpers.dropDatabase( "neo4j", cluster );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( "foo", cluster );
        CausalClusteringTestHelpers.assertDatabaseEventuallyDoesNotExist( "neo4j", cluster );

        cluster.systemTx( ( db, tx ) ->
        {
            // WHEN
            Result result = tx.execute( "CALL dbms.cluster.setDefaultDatabase('foo')" );
            Map<String,Object> row = result.next();
            // THEN
            assertThat( row.get( "result" ) ).isEqualTo( "Default database set to foo" );
            tx.commit();
        } );

        // THEN
        assertDefaultDatabase( "foo", cluster );
    }

    @Test
    void shouldChangeDatabaseWhenNewDefaultStopped() throws Exception
    {
        // GIVEN
        CausalClusteringTestHelpers.createDatabase( "foo", cluster, true );
        CausalClusteringTestHelpers.stopDatabase( "foo", cluster );
        CausalClusteringTestHelpers.stopDatabase( "neo4j", cluster );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStopped( "foo", cluster );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStopped( "neo4j", cluster );

        cluster.systemTx( ( db, tx ) ->
        {
            // WHEN
            Result result = tx.execute( "CALL dbms.cluster.setDefaultDatabase('foo')" );
            Map<String,Object> row = result.next();
            // THEN
            assertThat( row.get( "result" ) ).isEqualTo( "Default database set to foo" );
            tx.commit();
        } );

        // THEN
        assertDefaultDatabase( "foo", cluster );
    }

    @Test
    void shouldNotChangeDatabaseWhenOldDefaultOnline() throws Exception
    {
        // GIVEN
        CausalClusteringTestHelpers.createDatabase( "foo", cluster );
        assertDefaultDatabase( "neo4j", cluster );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( "foo", cluster );

        cluster.systemTx( ( db, tx ) ->
        {
            // WHEN
            assertThatThrownBy( () -> tx.execute( "CALL dbms.cluster.setDefaultDatabase('foo')" ) )
                    .hasMessageContaining( "The old default database neo4j is not fully stopped" );
        } );

        // THEN
        assertDefaultDatabase( "neo4j", cluster );
    }

    @Test
    void shouldFailWhenNewDefaultNotCreated() throws Exception
    {
        // GIVEN
        assertDefaultDatabase( "neo4j", cluster );

        cluster.systemTx( ( db, tx ) ->
        {
            // WHEN
            assertThatThrownBy( () -> tx.execute( "CALL dbms.cluster.setDefaultDatabase('foo')" ) )
                    .hasMessageContaining( "New default database foo does not exist." );
        } );

        // THEN
        assertDefaultDatabase( "neo4j", cluster );
    }

    @Test
    void shouldFailWhenNewDefaultDropped() throws Exception
    {
        // GIVEN
        CausalClusteringTestHelpers.createDatabase( "foo", cluster );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( "foo", cluster );
        CausalClusteringTestHelpers.dropDatabase( "foo", cluster );
        CausalClusteringTestHelpers.assertDatabaseEventuallyDoesNotExist( "foo", cluster );
        assertDefaultDatabase( "neo4j", cluster );

        cluster.systemTx( ( db, tx ) ->
        {
            // WHEN
            assertThatThrownBy( () -> tx.execute( "CALL dbms.cluster.setDefaultDatabase('foo')" ) )
                    .hasMessageContaining( "New default database foo does not exist." );
        } );

        // THEN
        assertDefaultDatabase( "neo4j", cluster );
    }

    @Test
    void shouldFailIfNotOnSystemDatabase() throws Exception
    {
        // GIVEN
        assertDefaultDatabase( "neo4j", cluster );

        cluster.coreTx( "neo4j", ( db, tx ) ->
        {
            // WHEN
            assertThatThrownBy( () -> tx.execute( "CALL dbms.cluster.setDefaultDatabase('foo')" ) ).hasMessageContaining(
                    "This is a system-only procedure and it should be executed against the system database: dbms.cluster.setDefaultDatabase" );
        } );

        // THEN
        assertDefaultDatabase( "neo4j", cluster );
    }

    @Test
    void shouldFailWithWrongParameters() throws Exception
    {
        // GIVEN
        assertDefaultDatabase( "neo4j", cluster );

        cluster.coreTx( "neo4j", ( db, tx ) ->
        {
            // WHEN .. THEN
            assertThatThrownBy( () -> tx.execute( "CALL dbms.cluster.setDefaultDatabase()" ) ).hasMessageContaining(
                    "Procedure call does not provide the required number of arguments: got 0 expected at least 1 (total: 1, 0 of which have default values)" );
        } );

        cluster.coreTx( "neo4j", ( db, tx ) ->
        {
            // WHEN .. THEN
            assertThatThrownBy( () -> tx.execute( "CALL dbms.cluster.setDefaultDatabase('foo', 'bar')" ) ).hasMessageContaining(
                    "Procedure call provides too many arguments: got 2 expected no more than 1" );
        } );

        cluster.coreTx( "neo4j", ( db, tx ) ->
        {
            // WHEN .. THEN
            assertThatThrownBy( () -> tx.execute( "CALL dbms.cluster.setDefaultDatabase(true)" ) ).hasMessageContaining(
                    "Type mismatch: expected String but was Boolean" );
        } );

        // THEN
        assertDefaultDatabase( "neo4j", cluster );
    }

    private void assertDefaultDatabase( String databaseName, Cluster cluster ) throws Exception
    {
        List<ShowDatabasesResultRow> rows = CausalClusteringTestHelpers.showDatabases( cluster );
        Set<String> defaultDbs = rows.stream()
                                     .filter( ShowDatabasesResultRow::isDefault )
                                     .map( ShowDatabasesResultRow::name )
                                     .collect( Collectors.toSet() );
        assertThat( defaultDbs ).containsOnly( databaseName );
    }
}
