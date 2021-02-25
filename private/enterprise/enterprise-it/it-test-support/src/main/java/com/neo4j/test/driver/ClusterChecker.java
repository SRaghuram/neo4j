/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.cluster.ClusterComposition;

import static org.assertj.core.api.Assertions.assertThat;

public final class ClusterChecker implements AutoCloseable
{
    private final ExecutorService executorService;
    private final Map<URI,Driver> driverCache;
    private static final Duration QUERY_TIMEOUT = Duration.ofMinutes( 5 );

    public static ClusterChecker fromBoltURIs( Collection<URI> directBoltAddresses, DriverSupplier driverFactory ) throws IOException
    {
        if ( directBoltAddresses.stream().anyMatch( uri -> !uri.getScheme().equalsIgnoreCase( "bolt" ) ) )
        {
            throw new IllegalArgumentException( "Direct bolt URIs are required." );
        }

        Map<URI,Driver> driverCache = new HashMap<>( directBoltAddresses.size() );
        for ( URI uri : directBoltAddresses )
        {
            driverCache.put( uri, driverFactory.supply( uri ) );
        }

        return new ClusterChecker( driverCache );
    }

    private ClusterChecker( Map<URI,Driver> driverCache )
    {
        this.driverCache = driverCache;
        executorService = Executors.newWorkStealingPool( driverCache.size() );
    }

    public void verifyConnectivity() throws ExecutionException, InterruptedException, TimeoutException
    {
        executorService.submit(
                () -> driverCache.values().parallelStream().forEach( Driver::verifyConnectivity )
        ).get( QUERY_TIMEOUT.getSeconds(), TimeUnit.SECONDS );
    }

    public int size()
    {
        return driverCache.size();
    }

    public List<Record> assertCypherResponseMatchesOnAllServers( String cypher ) throws ExecutionException, InterruptedException, TimeoutException
    {
        var results = runOnAllServers( cypher );
        List<Record> firstResult = results.entrySet().stream().findFirst().get().getValue();
        results.forEach(
                ( key, value ) -> assertThat( value ).as( "Response on: " + key.toString() )
                                                     .containsExactlyElementsOf( firstResult )
        );
        // If the results are all the same I only need to return one instance.
        return firstResult;
    }

    public <T> Map<URI,T> runOnAllServers( TransactionWork<T> tx ) throws ExecutionException, InterruptedException, TimeoutException
    {
        return executorService.submit(
                () -> driverCache.entrySet()
                                 .parallelStream()
                                 .collect( Collectors.toMap( Map.Entry::getKey, runTx( tx ) ) )
        ).get( QUERY_TIMEOUT.getSeconds(), TimeUnit.SECONDS );
    }

    public <T> Map<URI,List<Record>> runOnAllServers( String cypher ) throws ExecutionException, InterruptedException, TimeoutException
    {
        return executorService.submit(
                () -> driverCache.entrySet()
                                 .parallelStream()
                                 .collect( Collectors.toMap( Map.Entry::getKey, runCypher( cypher ) ) )
        ).get( QUERY_TIMEOUT.getSeconds(), TimeUnit.SECONDS );
    }

    public void verifyClusterStateMatchesOnAllServers() throws ExecutionException, InterruptedException, TimeoutException
    {
        // Check cluster.overview and SHOW DATABASES are consistent with one another
        var clusterOverviewResult = assertCypherResponseMatchesOnAllServers(
                "CALL dbms.cluster.overview() YIELD id, addresses, groups, databases " +
                "WITH id, addresses, groups, databases " +
                "WITH addresses[0] as bolt_address, keys(databases) as db_names, databases " +
                "UNWIND db_names as name " +
                "WITH name, split(bolt_address, '://')[1] as address, toLower(databases[name]) as role " +
                "RETURN name, address, role ORDER BY name + address + role"
        );

        var showDatabasesResult = assertCypherResponseMatchesOnAllServers(
                "SHOW DATABASES YIELD name, address, role ORDER BY name + address + role"
        );

        assertThat( clusterOverviewResult ).containsExactlyElementsOf( showDatabasesResult );

        Set<String> databaseNames = clusterOverviewResult.stream().map( r -> r.get( "name" ).asString() ).collect( Collectors.toSet() );
        Set<String> addressesFromOverview = clusterOverviewResult.stream().map( r -> r.get( "address" ).asString() ).collect( Collectors.toSet() );

        // Check that routing tables are consistent
        for ( String databaseName : databaseNames )
        {
            // Check that routing tables from each server match one another
            Map<URI,ClusterComposition> neo4jRoutingTableResults = runOnAllServers(
                    tx -> ClusterComposition.parse(
                            tx.run( "CALL dbms.routing.getRoutingTable({}, $name)", Map.of( "name", databaseName ) ).single(), 0L
                    )
            );
            Set<ClusterComposition> routingTables = Set.copyOf( neo4jRoutingTableResults.values() );
            assertThat( routingTables ).hasSize( 1 );

            // Check that the servers reported by the routing tables match the servers from cluster.overview & SHOW DATABASES
            Set<BoltServerAddress> neo4jServers = new HashSet<>( addressesFromOverview.size() );
            routingTables.forEach(
                    r ->
                    {
                        neo4jServers.addAll( r.readers() );
                        neo4jServers.addAll( r.writers() );
                    }
            );
            assertThat( neo4jServers.stream().map( BoltServerAddress::toString ) ).containsExactlyInAnyOrderElementsOf( addressesFromOverview );

            // TODO: check that the roles reported by the routing tables match the roles from SHOW DATABASES
        }
    }

    private <T> Function<Map.Entry<URI,Driver>,T> runTx( TransactionWork<T> tx )
    {
        return entrySet ->
        {
            try (
                    Session session = entrySet.getValue().session();
            )
            {
                return session.readTransaction( tx );
            }
        };
    }

    private Function<Map.Entry<URI,Driver>,List<Record>> runCypher( String cypher )
    {
        return uri -> runTx( tx -> tx.run( cypher ).list() ).apply( uri );
    }

    @Override
    public synchronized void close() throws ExecutionException, InterruptedException, TimeoutException
    {
        try
        {
            if ( !driverCache.isEmpty() )
            {
                executorService.submit(
                        () -> driverCache.values().parallelStream().forEach( Driver::close )
                ).get( QUERY_TIMEOUT.getSeconds(), TimeUnit.SECONDS );
                driverCache.clear();
            }
        }
        finally
        {
            shutdownExecutor( executorService );
        }
    }

    private static void shutdownExecutor( ExecutorService executor )
    {
        if ( executor.isTerminated() )
        {
            return;
        }
        try
        {
            if ( !executor.awaitTermination( 10, TimeUnit.SECONDS ) )
            {
                executor.shutdownNow();
                if ( !executor.awaitTermination( 3, TimeUnit.MINUTES ) )
                {
                    throw new RuntimeException( "ExecutorService did not shutdown: " + ClusterChecker.class.getName() );
                }
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private int numberOfServers()
    {
        return driverCache.size();
    }

    public boolean areLeadershipsWellBalanced( int expectedNumberOfDatabases ) throws InterruptedException, ExecutionException, TimeoutException
    {
        var results = assertCypherResponseMatchesOnAllServers( "SHOW DATABASES YIELD name, address, role WHERE role='leader'" );

        List<String> databases = results.stream().map( r -> r.get( "name" ).asString() ).distinct().collect( Collectors.toList() );
        assertThat( databases ).hasSize( expectedNumberOfDatabases );
        var totalNumberOfDatabases = databases.size();

        var dbsPerAddress = results.stream().collect( Collectors.groupingBy( r -> r.get( "address" ) ) );

        var mostDatabaseLeaderships = dbsPerAddress.values().stream().mapToInt( List::size ).max().getAsInt();
        var fewestDatabaseLeaderships = dbsPerAddress.size() < numberOfServers() ? 0
                                                                                 : dbsPerAddress.values().stream().mapToInt( List::size ).min().getAsInt();
        return mostDatabaseLeaderships == fewestDatabaseLeaderships || mostDatabaseLeaderships == fewestDatabaseLeaderships + 1;
    }
}
