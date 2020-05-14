/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryIdTest
{
    @Test
    void printsQueryIds() throws InvalidArgumentsException
    {
        assertThat( new QueryId( 12L ).toString() ).isEqualTo( "query-12" );
    }

    @Test
    void doesNotConstructNegativeQueryIds()
    {
        assertThrows( InvalidArgumentsException.class, () -> new QueryId( -15L ) );
    }

    @Test
    void parsesQueryIds() throws InvalidArgumentsException
    {
        assertThat( QueryId.parse( "query-14" ) ).isEqualTo( new QueryId( 14L ) );
    }

    @Test
    void doesNotParseNegativeQueryIds()
    {
        assertThrows( InvalidArgumentsException.class, () -> QueryId.parse( "query--12" ) );
    }

    @Test
    void doesNotParseWrongPrefix()
    {
        assertThrows( InvalidArgumentsException.class, () -> QueryId.parse( "querr-12" ) );
    }

    @Test
    void doesNotParseRandomText()
    {
        assertThrows( InvalidArgumentsException.class, () -> QueryId.parse( "blarglbarf" ) );
    }

    @Test
    void doesNotParseTrailingRandomText()
    {
        assertThrows( InvalidArgumentsException.class, () -> QueryId.parse( "query-12  " ) );
    }

    @Test
    void doesNotParseEmptyText()
    {
        assertThrows( InvalidArgumentsException.class, () -> QueryId.parse( "" ) );
    }
}
