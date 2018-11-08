/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.util.stream.Stream;

public enum SelectedBackupProtocol
{
    ANY( "any" ),
    CATCHUP( "catchup" );

    public String getName()
    {
        return name;
    }

    private final String name;

    SelectedBackupProtocol( String name )
    {
        this.name = name;
    }

    public static SelectedBackupProtocol fromUserInput( String value )
    {
        return Stream.of( SelectedBackupProtocol.values() )
                .filter( proto -> value.equals( proto.name ) )
                .findFirst()
                .orElseThrow( () -> new RuntimeException( String.format( "Failed to parse `%s`", value ) ) );
    }
}
