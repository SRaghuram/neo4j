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
package org.neo4j.configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.ConfigValue.valueToString;

class ConfigValueTest
{
    @Test
    void handlesEmptyValue()
    {
        ConfigValue value = new ConfigValue( "name", Optional.empty(), Optional.empty(), Optional.empty(),
                "description", false, false, false, Optional.empty(), false );

        assertEquals( Optional.empty(), value.value() );
        assertEquals( "null", value.toString() );
        assertFalse( value.deprecated() );
        assertEquals( Optional.empty(), value.replacement() );
        assertFalse( value.internal() );
        assertFalse( value.secret() );
    }

    @Test
    void handlesInternal()
    {
        ConfigValue value = new ConfigValue( "name", Optional.empty(), Optional.empty(), Optional.empty(),
                "description", true, false, false, Optional.empty(), false );

        assertTrue( value.internal() );
        assertFalse( value.secret() );
    }

    @Test
    void handlesNonEmptyValue()
    {
        ConfigValue value = new ConfigValue( "name", Optional.empty(), Optional.empty(), Optional.of( 1 ),
                "description", false, false, false, Optional.empty(), false );

        assertEquals( Optional.of( 1 ), value.value() );
        assertEquals( "1", value.toString() );
        assertFalse( value.deprecated() );
        assertEquals( Optional.empty(), value.replacement() );
        assertFalse( value.internal() );
        assertFalse( value.secret() );
    }

    @Test
    void handlesDeprecationAndReplacement()
    {
        ConfigValue value = new ConfigValue( "old_name", Optional.empty(), Optional.empty(), Optional.of( 1 ),
                "description", false, false, true, Optional.of( "new_name" ), false );

        assertEquals( Optional.of( 1 ), value.value() );
        assertEquals( "1", value.toString() );
        assertTrue( value.deprecated() );
        assertEquals( "new_name", value.replacement().get() );
        assertFalse( value.internal() );
        assertFalse( value.secret() );
    }

    @Test
    void handlesValueDescription()
    {
        ConfigValue value = new ConfigValue( "old_name", Optional.empty(), Optional.empty(), Optional.of( 1 ),
                "a simple integer", false, false, true, Optional.of( "new_name" ), false );

        assertEquals( Optional.of( 1 ), value.value() );
        assertEquals( "1", value.toString() );
        assertTrue( value.deprecated() );
        assertEquals( "new_name", value.replacement().get() );
        assertFalse( value.internal() );
        assertFalse( value.secret() );
        assertEquals( "a simple integer", value.valueDescription() );
    }

    @Test
    void handlesSecretValue()
    {
        ConfigValue value = new ConfigValue( "name", Optional.empty(), Optional.empty(), Optional.of( "secret" ),
                "description", false, false, false, Optional.empty(), true );

        assertEquals( Optional.of( "secret" ), value.value() );
        assertEquals( Secret.OBFUSCATED, value.toString() );
        assertFalse( value.deprecated() );
        assertEquals( Optional.empty(), value.replacement() );
        assertFalse( value.internal() );
        assertTrue( value.secret() );
    }

    @Test
    void durationValueIsRepresentedWithUnit()
    {
        assertEquals( "120000ms", valueToString( Duration.ofMinutes( 2 ) ) );
    }

    @Test
    void stringValueIsRepresentedAsString()
    {
        assertEquals( "bob", valueToString( "bob" ) );
    }

    @Test
    void intValueIsRepresentedAsInt()
    {
        assertEquals( "7", valueToString( 7 ) );
    }

    @Test
    void nullIsHandled()
    {
        assertEquals( "null", valueToString( null ) );
    }
}
