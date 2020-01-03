/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.helpers.DatabaseNameValidator.MAXIMUM_DATABASE_NAME_LENGTH;

class DbmsQueryIdTest
{
    @Test
    void printsQueryIds() throws InvalidArgumentsException
    {
        assertThat( new DbmsQueryId( "neo4j", 12L ).toString(), equalTo( "neo4j-query-12" ) );
    }

    @Test
    void doesNotConstructNegativeQueryIds()
    {
        assertThrows( InvalidArgumentsException.class, () -> new DbmsQueryId( "neo4j", -15L ) );
    }

    @Test
    void parsesQueryIds() throws InvalidArgumentsException
    {
        assertThat( new DbmsQueryId( "neo4j-query-14" ), equalTo( new DbmsQueryId( "neo4j", 14L ) ) );
    }

    @Test
    void doesNotParseNegativeQueryIds()
    {
        assertThrows( InvalidArgumentsException.class, () -> new DbmsQueryId( "neo4j-query--12" ) );
    }

    @Test
    void doesNotParseRandomText()
    {
        assertThrows( InvalidArgumentsException.class, () -> new DbmsQueryId( "blarglbarf" ) );
    }

    @Test
    void doesNotParseTrailingRandomText()
    {
        assertThrows( InvalidArgumentsException.class, () -> new DbmsQueryId( "neo4j-query-12  " ) );
    }

    @Test
    void doesNotParseEmptyText()
    {
        assertThrows( InvalidArgumentsException.class, () -> new DbmsQueryId( "" ) );
    }

    @Test
    void validateAndNormalizeDatabaseName() throws InvalidArgumentsException
    {
        assertThat( new DbmsQueryId( "NEO4J-query-14" ), equalTo( new DbmsQueryId( "neo4j", 14L ) ) );
        IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () ->
                new DbmsQueryId( "a".repeat( MAXIMUM_DATABASE_NAME_LENGTH + 1 ) + "-query-14" ) );
        assertThat( e.getMessage(), containsString( " must have a length between " ) );
    }
}
