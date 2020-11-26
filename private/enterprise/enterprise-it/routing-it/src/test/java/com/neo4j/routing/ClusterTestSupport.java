/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.routing;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

public abstract class ClusterTestSupport
{
    private Cluster cluster;
    private List<Driver> coreDrivers;
    private Driver readReplicaDriver;

    protected Driver fooLeaderDriver;
    protected Driver fooFollowerDriver;
    protected Driver systemFollowerDriver;

    protected final List<Bookmark> bookmarks = new ArrayList<>();

    void beforeEach( RoutingContext routingContext, Cluster cluster, List<Driver> coreDrivers, Driver readReplicaDriver ) throws TimeoutException
    {
        this.cluster = cluster;
        this.coreDrivers = coreDrivers;
        this.readReplicaDriver = readReplicaDriver;

        when( routingContext.isServerRoutingEnabled() ).thenReturn( true );

        fooLeaderDriver = getFooLeaderDriver();
        fooFollowerDriver = getFooFollowerDriver();
        systemFollowerDriver = getFollowerDriver( "system" );

        run( fooLeaderDriver, "foo", AccessMode.WRITE, session ->
        {
            session.writeTransaction( tx -> tx.run( "MATCH (n) DETACH DELETE n" ) );
            session.writeTransaction( tx -> tx.run( "CREATE (:Person {name: 'Anna', uid: 0, age: 30})" ).consume() );
            session.writeTransaction( tx -> tx.run( "CREATE (:Person {name: 'Bob',  uid: 1, age: 40})" ).consume() );
            return null;
        } );
    }

    protected void doTestWriteOnClusterMember( Driver writeDriver )
    {
        var createQuery = joinAsLines(
                "USE foo",
                "CREATE (c:Person {name: 'Carrie',  uid: 2, age: 50})",
                "CREATE (d:Person {name: 'Dave',  uid: 3, age: 60})",
                "WITH *",
                "UNWIND [c, d] AS person",
                "RETURN person.name AS name" );
        var fooWriterResult = run( writeDriver, "neo4j", AccessMode.WRITE,
                                   session -> session.writeTransaction( tx -> tx.run(  createQuery ).list() ) );
        assertEquals( List.of( "Carrie", "Dave" ), getNames( fooWriterResult ) );

        var matchQueryWithoutUse = joinAsLines(
                "MATCH (person:Person )",
                "RETURN person.name AS name"
        );

        var neo4jResult = run( fooLeaderDriver, "neo4j", AccessMode.READ,
                               session -> session.writeTransaction( tx -> tx.run( matchQueryWithoutUse ).list() ));

        // we should get nothing when asking neo4j database
        assertEquals( List.of(), neo4jResult );

        var matchQueryWithUse = joinAsLines(
                "USE foo",
                "MATCH (person:Person )",
                "RETURN person.name AS name"
        );

        var fooFollowerResult = run( fooFollowerDriver, "neo4j", AccessMode.READ,
                                     session -> session.writeTransaction( tx -> tx.run( matchQueryWithUse ).list() ));
        assertEquals( List.of( "Anna", "Bob", "Carrie", "Dave" ), getNames( fooFollowerResult ) );

        var fooReadReplicaResult = run( readReplicaDriver, "neo4j", AccessMode.READ,
                                        session -> session.writeTransaction( tx -> tx.run( matchQueryWithUse ).list() ));
        assertEquals( List.of( "Anna", "Bob", "Carrie", "Dave" ), getNames( fooReadReplicaResult ) );
    }

    protected <T> T run( Driver driver, String databaseName, AccessMode mode, Function<Session,T> workload )
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

    protected List<String> getNames( List<Record> records )
    {
        return records.stream()
                      .map( record -> record.get( "name" ) )
                      .map( Value::asString )
                      .sorted()
                      .collect( Collectors.toList() );
    }

    protected Driver getFooFollowerDriver() throws TimeoutException
    {
        return getFollowerDriver( "foo" );
    }

    protected Driver getFollowerDriver( String databaseName ) throws TimeoutException
    {
        var fooLeader = cluster.awaitLeader( databaseName );
        var fooFollower = getFollower( cluster, fooLeader );
        return coreDrivers.get( fooFollower.index() );
    }

    protected Driver getFooLeaderDriver() throws TimeoutException
    {
        var fooLeader = cluster.awaitLeader( "foo" );
        return coreDrivers.get( fooLeader.index() );
    }

    protected static void awaitDbAvailable( Cluster cluster, String databaseName )
    {
        awaitDbExists( cluster, databaseName );
        boolean availableEverywhere = cluster.allMembers().stream()
                                             .allMatch( member -> member.managementService().database( databaseName ).isAvailable( 60_000 ) );
        assertTrue( availableEverywhere );
    }

    private static void awaitDbExists( Cluster cluster, String databaseName )
    {
        var start = Instant.now();

        while ( true )
        {
            try
            {
                cluster.allMembers().forEach( member -> member.managementService().database( databaseName ) );
                return;
            }
            catch ( DatabaseNotFoundException e )
            {
                if ( Duration.between( start, Instant.now() ).toMinutes() > 1 )
                {
                    throw new IllegalStateException( "Database does not exist", e );
                }
            }

            try
            {
                Thread.sleep( 1 );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    protected static Driver getReadReplicaDriver( Cluster cluster )
    {
        var readReplica = cluster.findAnyReadReplica();
        return driver( readReplica.directURI() );
    }

    static CoreClusterMember getFollower( Cluster cluster, CoreClusterMember leader )
    {
        return cluster.coreMembers().stream()
                      .filter( member -> member.index() != leader.index() )
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
