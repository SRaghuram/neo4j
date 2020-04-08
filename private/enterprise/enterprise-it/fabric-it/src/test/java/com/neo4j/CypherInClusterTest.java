/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.driver.DriverExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

@TestDirectoryExtension
@ExtendWith( FabricEverywhereExtension.class )
@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@DriverExtension
class CypherInClusterTest
{
    @Inject
    private static ClusterFactory clusterFactory;

    private static Cluster cluster;

    private static Map<Integer,Driver> coreDrivers = new HashMap<>();

    private static Driver fooLeaderDriver;
    private static Driver fooFollowerDriver;

    private static Driver readReplicaDriver;

    private final List<Bookmark> bookmarks = new ArrayList<>();

    @BeforeAll
    static void beforeAll() throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 2 )
                .withNumberOfReadReplicas( 1 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        var readReplica = cluster.findAnyReadReplica();
        readReplicaDriver = driver( readReplica.directURI() );

        cluster.coreMembers().forEach( core -> coreDrivers.put( core.serverId(), driver( core.directURI() ) ) );

        var systemLeader = cluster.awaitLeader( "system" );
        Driver systemLeaderDriver = coreDrivers.get( systemLeader.serverId() );

        try ( var session = systemLeaderDriver.session( SessionConfig.builder().withDatabase( "system" ).build() ) )
        {
            session.run( "CREATE DATABASE foo" ).consume();
        }

        var fooLeader = cluster.awaitLeader( "foo" );
        awaitDbAvailable( "foo" );

        fooLeaderDriver = coreDrivers.get( fooLeader.serverId() );
        var fooFollower = getFollower( fooLeader );
        fooFollowerDriver = coreDrivers.get( fooFollower.serverId() );

    }

    @BeforeEach
    void beforeEach()
    {
        bookmarks.clear();

        run( fooLeaderDriver, "foo", AccessMode.WRITE, session ->
        {
            session.run( "MATCH (n) DETACH DELETE n" );
            session.run( "CREATE (:Person {name: 'Anna', uid: 0, age: 30})" ).consume();
            session.run( "CREATE (:Person {name: 'Bob',  uid: 1, age: 40})" ).consume();
            return null;
        } );
    }

    @AfterAll
    static void afterAll()
    {
        List.<Runnable>of(
                () -> coreDrivers.get( 0 ).close(),
                () -> coreDrivers.get( 1 ).close(),
                () -> readReplicaDriver.close(),
                () -> cluster.shutdown()
        ).parallelStream().forEach( Runnable::run );
    }

    @Test
    void testBasicInteraction()
    {
        var createQuery = joinAsLines(
                "USE foo",
                "CREATE (:Person {name: 'Carrie',  uid: 2, age: 50})" );
        run( fooLeaderDriver, "neo4j", AccessMode.WRITE, session -> {
            session.run( createQuery ).consume();
            return null;
        });

        var matchQueryWithoutUSe = joinAsLines(
                "MATCH (person:Person )",
                "RETURN person.name AS name"
        );

        var neo4jResult = run( fooLeaderDriver, "neo4j", AccessMode.READ, session -> session.run( matchQueryWithoutUSe ).list() );

        // we should get nothing when asking neo4j database
        assertEquals( List.of(), neo4jResult );

        var matchQueryWithUse = joinAsLines(
                "USE foo",
                "MATCH (person:Person )",
                "RETURN person.name AS name"
        );

        var fooFollowerResult = run( fooFollowerDriver, "neo4j", AccessMode.READ, session -> session.run( matchQueryWithUse ).list() );
        assertEquals( List.of( "Anna", "Bob", "Carrie"), getNames( fooFollowerResult ) );

        var fooReadReplicaResult = run( readReplicaDriver, "neo4j", AccessMode.READ, session -> session.run( matchQueryWithUse ).list() );
        assertEquals( List.of( "Anna", "Bob", "Carrie"), getNames( fooReadReplicaResult ) );

    }

    private static void awaitDbAvailable( String databaseName )
    {
        boolean availableEverywhere = cluster.allMembers().stream()
                .allMatch(  member -> member.managementService().database( databaseName ).isAvailable( 60_000 ) );
        assertTrue( availableEverywhere );
    }

    private List<String> getNames( List<Record> records )
    {
        return  records.stream()
                .map( record -> record.get( "name" ) )
                .map( Value::asString )
                .sorted()
                .collect( Collectors.toList());
    }

    private <T> T run( Driver driver, String databaseName, AccessMode mode, Function<Session,T> workload )
    {
        try ( var session = driver.session( SessionConfig.builder()
                .withDatabase( databaseName )
                .withDefaultAccessMode( mode )
                .withBookmarks( bookmarks ).build() ) )
        {
            T value = workload.apply( session );
            bookmarks.add( session.lastBookmark() );
            return value;
        }
    }

    static CoreClusterMember getFollower( CoreClusterMember leader )
    {
        return cluster.coreMembers().stream()
                .filter( member -> member.serverId() != leader.serverId() )
                .findAny()
                .get();
    }

    static Driver driver( String uri )
    {
        return GraphDatabase.driver(
                uri,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );
    }
}
