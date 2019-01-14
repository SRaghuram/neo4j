/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.multi_cluster.procedure;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.Role;
import org.neo4j.causalclustering.routing.multi_cluster.MultiClusterRoutingResult;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.routing.procedure.RoutingResultFormatHelper.parseEndpoints;

/**
 * The result format of {@link GetRoutersForDatabaseProcedure} and
 * {@link GetRoutersForAllDatabasesProcedure} procedures.
 */
public class MultiClusterRoutingResultFormat
{

    private static final String DB_NAME_KEY = "database";
    private static final String ADDRESSES_KEY = "addresses";

    private MultiClusterRoutingResultFormat()
    {
    }

    static Object[] build( MultiClusterRoutingResult result )
    {
        Function<List<Endpoint>, Object[]> stringifyAddresses = es ->
                es.stream().map( e -> e.address().toString() ).toArray();

        List<Map<String,Object>> response = result.routers().entrySet().stream().map( entry ->
        {
            String dbName = entry.getKey();
            Object[] addresses = stringifyAddresses.apply( entry.getValue() );

            Map<String,Object> responseRow = new TreeMap<>();

            responseRow.put( DB_NAME_KEY, dbName );
            responseRow.put( ADDRESSES_KEY, addresses );

            return responseRow;
        } ).collect( Collectors.toList() );

        long ttlSeconds = MILLISECONDS.toSeconds( result.ttlMillis() );
        return new Object[]{ttlSeconds, response};
    }

    public static MultiClusterRoutingResult parse( Map<String,Object> record )
    {
        return parse( new Object[]{
                record.get( ParameterNames.TTL.parameterName() ),
                record.get( ParameterNames.ROUTERS.parameterName() )
        } );
    }

    public static MultiClusterRoutingResult parse( Object[] record )
    {
        long ttlSeconds = (long) record[0];
        @SuppressWarnings( "unchecked" )
        List<Map<String,Object>> rows = (List<Map<String,Object>>) record[1];
        Map<String,List<Endpoint>> routers = parseRouters( rows );

        return new MultiClusterRoutingResult( routers, ttlSeconds * 1000 );
    }

    private static Map<String,List<Endpoint>> parseRouters( List<Map<String,Object>> responseRows )
    {
        Function<Map<String,Object>,String> dbNameFromRow = row -> (String) row.get( DB_NAME_KEY );
        Function<Map<String,Object>,List<Endpoint>> endpointsFromRow =
                row -> parseEndpoints( (Object[]) row.get( ADDRESSES_KEY ), Role.ROUTE );
        return responseRows.stream().collect( Collectors.toMap( dbNameFromRow, endpointsFromRow ) );
    }
}
