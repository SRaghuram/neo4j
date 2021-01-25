/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.logging.internal;

import org.junit.jupiter.api.Test;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PrefixedLogTest
{
    private final String prefix = "prefix";

    @Test
    void shouldContainPrefix()
    {
        var mockedLog = mock( Log.class );
        var prefixedLog = new PrefixedLog( prefix, mockedLog );

        var message = "message";
        prefixedLog.info( message );

        verify( mockedLog ).info( format( "[%s] %s", prefix, message ) );
    }

    @Test
    void shouldThrowIfNullPrefix()
    {
        assertThrows( NullPointerException.class, () -> new PrefixedLog( null, NullLog.getInstance() ) );
    }

    @Test
    void shouldThrowIfNullLog()
    {
        assertThrows( NullPointerException.class, () -> new PrefixedLog( prefix, null ) );
    }
}
