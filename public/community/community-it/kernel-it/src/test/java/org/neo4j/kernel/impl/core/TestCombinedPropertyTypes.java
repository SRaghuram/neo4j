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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestCombinedPropertyTypes extends AbstractNeo4jTestCase
{
    private Node node1;

    @BeforeEach
    void createInitialNode()
    {
        node1 = getGraphDb().createNode();
    }

    @AfterEach
    void deleteInitialNode()
    {
        node1.delete();
    }

    @Test
    void testDateTypeOrdinalDayWithPrecedingInLinedLong()
    {
        testDateTypeWithPrecedingInLinedLong( DateValue.ordinalDate( 4800, 1 ) );
    }

    @Test
    void testDateTypeOrdinalDayWithPrecedingNotInLinedLong()
    {
        testDateTypeWithPrecedingNotInLinedLong( DateValue.ordinalDate( 4800, 1 ) );
    }

    @Test
    void testLocalTimeWithPrecedingInLinedLong()
    {
        testDateTypeWithPrecedingInLinedLong( LocalTimeValue.parse( "13:45:02" ) );
    }

    @Test
    void testLocalTimeWithPrecedingNotInLinedLong()
    {
        testDateTypeWithPrecedingNotInLinedLong( LocalTimeValue.parse( "13:45:02" ) );
    }

    @Test
    void testDateTimeWithPrecedingInLinedLong()
    {
        testDateTypeWithPrecedingInLinedLong(
                DateTimeValue.datetime( DateValue.parse( "2018-04-01" ), LocalTimeValue.parse( "01:02:03" ), ZoneId.of( "Europe/Stockholm" ) ) );
    }

    @Test
    void testDateTimeWithPrecedingNotInLinedLong()
    {
        testDateTypeWithPrecedingNotInLinedLong(
                DateTimeValue.datetime( DateValue.parse( "2018-04-01" ), LocalTimeValue.parse( "01:02:03" ), ZoneId.of( "Europe/Stockholm" ) ) );
    }

    private void testDateTypeWithPrecedingInLinedLong( Value value )
    {
        node1.setProperty( "l1", 255 ); // Setting these low bits was triggering a bug in some date types decision on formatting
        String key = "dt";
        node1.setProperty( key, value );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( value.asObjectCopy(), property );
    }

    private void testDateTypeWithPrecedingNotInLinedLong( Value value )
    {
        node1.setProperty( "l1", Long.MAX_VALUE );
        String key = "dt";
        node1.setProperty( key, value );
        newTransaction();

        Object property = node1.getProperty( key );
        assertEquals( value.asObjectCopy(), property );
    }
}
