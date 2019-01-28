/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v4.messaging;

import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;

class PullNMessageTest
{
    @Test
    void shouldParsePullNMetadataCorrectly() throws Throwable
    {
        // When
        PullNMessage message = new PullNMessage( asMapValue( singletonMap( "n", 100L ) ) );

        // Then
        assertThat( message.n(), equalTo( 100L ) );
    }

    @Test
    void shouldThrowExceptionIfFailedToParseTransactionMetadataCorrectly() throws Throwable
    {
        // Given
        Map<String,Object> msgMetadata = map( "n", "invalid value type" );
        MapValue meta = ValueUtils.asMapValue( msgMetadata );
        // When & Then
        BoltIOException exception = assertThrows( BoltIOException.class, () -> new PullNMessage( meta ) );
        assertThat( exception.getMessage(), startsWith( "Expecting PULL_N size n to be a Long value, but got: String(\"invalid value type\")" ) );
    }

    @Test
    void shouldThrowExceptionIfMissingMeta() throws Throwable
    {
        // When & Then
        BoltIOException exception = assertThrows( BoltIOException.class, () -> new PullNMessage( MapValue.EMPTY ) );
        assertThat( exception.getMessage(), startsWith( "Expecting PULL_N size n to be a Long value, but got: NO_VALUE" ) );
    }

    @Test
    void shouldBeEqual() throws Throwable
    {
        // Given
        PullNMessage message = new PullNMessage( asMapValue( singletonMap( "n", 100L ) ) );

        PullNMessage messageEqual = new PullNMessage( asMapValue( singletonMap( "n", 100L ) ) );

        // When & Then
        assertEquals( message, messageEqual );
    }

    @Test
    void shouldNotBeEqualWithDiscardN() throws Throwable
    {
        // Given
        PullNMessage pull = new PullNMessage( asMapValue( singletonMap( "n", 100L ) ) );

        DiscardNMessage discard = new DiscardNMessage( asMapValue( singletonMap( "n", 100L ) ) );

        // When & Then
        assertNotEquals( pull, discard );
    }
}
