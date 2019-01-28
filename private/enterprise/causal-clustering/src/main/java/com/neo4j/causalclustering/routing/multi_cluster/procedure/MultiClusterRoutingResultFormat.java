/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.multi_cluster.procedure;

import com.neo4j.causalclustering.routing.Endpoint;
import com.neo4j.causalclustering.routing.Role;
import com.neo4j.causalclustering.routing.multi_cluster.MultiClusterRoutingResult;
import com.neo4j.causalclustering.routing.procedure.RoutingResultFormatHelper;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

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

    static AnyValue[] build( MultiClusterRoutingResult result )
    {
        Function<List<Endpoint>, AnyValue[]> stringifyAddresses = es ->
                es.stream().map( e -> stringValue(e.address().toString()) ).toArray(AnyValue[]::new);

        List<AnyValue> response = result.routers().entrySet().stream().map( entry ->
        {
            String dbName = entry.getKey();
            AnyValue[] addresses = stringifyAddresses.apply( entry.getValue() );

            MapValueBuilder responseRow = new MapValueBuilder();

            responseRow.add( DB_NAME_KEY, stringValue(dbName) );
            responseRow.add( ADDRESSES_KEY, VirtualValues.list(addresses) );

            return responseRow.build();
        } ).collect( Collectors.toList() );

        LongValue ttlSeconds = longValue( MILLISECONDS.toSeconds( result.ttlMillis() ) );
        return new AnyValue[]{ttlSeconds, VirtualValues.fromList( response )};
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
                row -> RoutingResultFormatHelper
                        .parseEndpoints( ValueUtils.asListValue( (Iterable<?>) row.get( ADDRESSES_KEY ) ), Role.ROUTE );
        return responseRows.stream().collect( Collectors.toMap( dbNameFromRow, endpointsFromRow ) );
    }
}
