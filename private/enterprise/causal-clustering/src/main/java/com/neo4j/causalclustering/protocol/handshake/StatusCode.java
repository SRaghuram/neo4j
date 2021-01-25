/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * General status codes sent in responses.
 */
public enum StatusCode
{
    SUCCESS( 0 ),
    ONGOING( 1 ),
    FAILURE( -1 );

    private final int codeValue;
    private static final AtomicReference<Map<Integer, StatusCode>> CODE_MAP = new AtomicReference<>();

    StatusCode( int codeValue )
    {
        this.codeValue = codeValue;
    }

    public int codeValue()
    {
        return codeValue;
    }

    public static Optional<StatusCode> fromCodeValue( int codeValue )
    {
        Map<Integer,StatusCode> map = CODE_MAP.get();
        if ( map == null )
        {
             map = Stream.of( StatusCode.values() )
                    .collect( Collectors.toMap( StatusCode::codeValue, Function.identity() ) );

            CODE_MAP.compareAndSet( null, map );
        }
        return Optional.ofNullable( map.get( codeValue ) );
    }
}
