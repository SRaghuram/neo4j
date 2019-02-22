/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.builtinprocs;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static com.neo4j.kernel.enterprise.builtinprocs.QueryId.fromExternalString;
import static com.neo4j.kernel.enterprise.builtinprocs.QueryId.ofInternalId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryIdTest
{
    @Test
    void printsQueryIds() throws InvalidArgumentsException
    {
        assertThat( ofInternalId( 12L ).toString(), equalTo( "query-12" ) );
    }

    @Test
    void doesNotConstructNegativeQueryIds()
    {
        assertThrows( InvalidArgumentsException.class, () -> ofInternalId( -15L ) );
    }

    @Test
    void parsesQueryIds() throws InvalidArgumentsException
    {
        assertThat( fromExternalString( "query-14" ), equalTo( ofInternalId( 14L ) ) );
    }

    @Test
    void doesNotParseNegativeQueryIds()
    {
        assertThrows( InvalidArgumentsException.class, () -> fromExternalString( "query--12" ) );
    }

    @Test
    void doesNotParseRandomText()
    {
        assertThrows( InvalidArgumentsException.class, () -> fromExternalString( "blarglbarf" ) );
    }

    @Test
    void doesNotParseTrailingRandomText()
    {
        assertThrows( InvalidArgumentsException.class, () -> fromExternalString( "query-12  " ) );
    }

    @Test
    void doesNotParseEmptyText()
    {
        assertThrows( InvalidArgumentsException.class, () -> fromExternalString( "" ) );
    }
}
