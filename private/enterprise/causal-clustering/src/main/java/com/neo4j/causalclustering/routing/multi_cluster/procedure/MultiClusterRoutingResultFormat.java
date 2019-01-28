/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.multi_cluster.procedure;

import com.neo4j.causalclustering.routing.Endpoint;
import com.neo4j.causalclustering.routing.Role;
import com.neo4j.causalclustering.routing.multi_cluster.MultiClusterRoutingResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static com.neo4j.causalclustering.routing.procedure.RoutingResultFormatHelper.parseEndpoints;
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

    public static MultiClusterRoutingResult parse( MapValue record )
    {
        return parse( new AnyValue[]{
                record.get( ParameterNames.TTL.parameterName() ),
                record.get( ParameterNames.ROUTERS.parameterName() )
        } );
    }

    public static MultiClusterRoutingResult parse( AnyValue[] record )
    {
        long ttlSeconds = ((LongValue) record[0]).longValue();
        @SuppressWarnings( "unchecked" )
        ListValue rows = (ListValue) record[1];
        Map<String,List<Endpoint>> routers = parseRouters( rows );

        return new MultiClusterRoutingResult( routers, ttlSeconds * 1000 );
    }

    private static Map<String,List<Endpoint>> parseRouters( ListValue responseRows )
    {
        Map<String, List<Endpoint>> routers = new HashMap<>(  );
        for ( AnyValue row : responseRows )
        {
            MapValue map = (MapValue) row;
            routers.put( ((TextValue) map.get( DB_NAME_KEY )).stringValue(),
                    parseEndpoints( (ListValue) map.get( ADDRESSES_KEY ), Role.ROUTE ) );
        }
        return routers;
    }
}
