/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.routing.Endpoint;
import com.neo4j.causalclustering.routing.Role;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingResult;
import com.neo4j.causalclustering.routing.procedure.RoutingResultFormatHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

/**
 * The result format of GetServersV1 and GetServersV2 procedures.
 */
public class ResultFormatV1
{
    private static final String ROLE_KEY = "role";
    private static final String ADDRESSES_KEY = "addresses";

    private ResultFormatV1()
    {
    }

    static AnyValue[] build( LoadBalancingProcessor.Result result )
    {
        AnyValue[] routers = result.routeEndpoints().stream()
                .map( Endpoint::address ).map( a -> stringValue(a.toString()) ).toArray( AnyValue[]::new );
        AnyValue[] readers = result.readEndpoints().stream()
                .map( Endpoint::address ).map(  a -> stringValue(a.toString()) ).toArray( AnyValue[]::new );
        AnyValue[] writers = result.writeEndpoints().stream()
                .map( Endpoint::address ).map(  a -> stringValue(a.toString()) ).toArray( AnyValue[]::new );

        List<AnyValue> servers = new ArrayList<>();

        if ( writers.length > 0 )
        {
            MapValueBuilder builder = new MapValueBuilder(  );

            builder.add( ROLE_KEY, stringValue( Role.WRITE.name() ));
            builder.add( ADDRESSES_KEY, VirtualValues.list( writers ) );

            servers.add( builder.build() );
        }

        if ( readers.length > 0 )
        {
            MapValueBuilder builder = new MapValueBuilder(  );

            builder.add( ROLE_KEY, stringValue( Role.READ.name() ));
            builder.add( ADDRESSES_KEY,  VirtualValues.list( readers ) );

            servers.add( builder.build() );
        }

        if ( routers.length > 0 )
        {
            MapValueBuilder builder = new MapValueBuilder(  );

            builder.add( ROLE_KEY, stringValue( Role.ROUTE.name() ) );
            builder.add( ADDRESSES_KEY, VirtualValues.list( routers ) );

            servers.add( builder.build() );
        }

        LongValue timeToLiveSeconds = longValue(MILLISECONDS.toSeconds( result.ttlMillis() ));
        return new AnyValue[]{timeToLiveSeconds, VirtualValues.fromList( servers )};
    }

    public static LoadBalancingResult parse( AnyValue[] record )
    {
        long timeToLiveSeconds = ((LongValue) record[0]).longValue();
        @SuppressWarnings( "unchecked" )
        ListValue endpointData = (ListValue) record[1];

        Map<Role,List<Endpoint>> endpoints = parseRows( endpointData );

        return new LoadBalancingResult(
                endpoints.get( Role.ROUTE ),
                endpoints.get( Role.WRITE ),
                endpoints.get( Role.READ ),
                timeToLiveSeconds * 1000 );
    }

    public static LoadBalancingResult parse( MapValue record )
    {
        return parse( new AnyValue[]{
                record.get( ParameterNames.TTL.parameterName() ),
                record.get( ParameterNames.SERVERS.parameterName() )
        } );
    }

    private static Map<Role,List<Endpoint>> parseRows( ListValue result )
    {
        Map<Role,List<Endpoint>> endpoints = new HashMap<>();
        for ( AnyValue single : result )
        {
            MapValue map = (MapValue)single;
            Role role = Role.valueOf( ((TextValue) map.get( "role" )).stringValue() );
            List<Endpoint> addresses = RoutingResultFormatHelper.parseEndpoints( (ListValue) map.get( "addresses" ), role );
            endpoints.put( role, addresses );
        }

        Arrays.stream( Role.values() ).forEach( r -> endpoints.putIfAbsent( r, Collections.emptyList() ) );

        return endpoints;
    }

}
