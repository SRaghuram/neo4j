/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure.commercial.builtin;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryIdTest
{
    @Test
    void printsQueryIds() throws InvalidArgumentsException
    {
        MatcherAssert.assertThat( QueryId.ofInternalId( 12L ).toString(), equalTo( "query-12" ) );
    }

    @Test
    void doesNotConstructNegativeQueryIds()
    {
        assertThrows( InvalidArgumentsException.class, () -> QueryId.ofInternalId( -15L ) );
    }

    @Test
    void parsesQueryIds() throws InvalidArgumentsException
    {
        MatcherAssert.assertThat( QueryId.fromExternalString( "query-14" ), equalTo( QueryId.ofInternalId( 14L ) ) );
    }

    @Test
    void doesNotParseNegativeQueryIds()
    {
        assertThrows( InvalidArgumentsException.class, () -> QueryId.fromExternalString( "query--12" ) );
    }

    @Test
    void doesNotParseRandomText()
    {
        assertThrows( InvalidArgumentsException.class, () -> QueryId.fromExternalString( "blarglbarf" ) );
    }

    @Test
    void doesNotParseTrailingRandomText()
    {
        assertThrows( InvalidArgumentsException.class, () -> QueryId.fromExternalString( "query-12  " ) );
    }

    @Test
    void doesNotParseEmptyText()
    {
        assertThrows( InvalidArgumentsException.class, () -> QueryId.fromExternalString( "" ) );
    }
}
