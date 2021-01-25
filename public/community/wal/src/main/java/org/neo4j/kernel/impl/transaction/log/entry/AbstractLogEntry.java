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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.util.TimeZone;

import org.neo4j.internal.helpers.Format;

public abstract class AbstractLogEntry implements LogEntry
{
    private final byte version;
    private final byte type;

    AbstractLogEntry( byte version, byte type )
    {
        this.type = type;
        this.version = version;
    }

    @Override
    public byte getType()
    {
        return type;
    }

    @Override
    public byte getVersion()
    {
        return version;
    }

    @Override
    public String toString( TimeZone timeZone )
    {
        return toString();
    }

    @Override
    public String timestamp( long timeWritten, TimeZone timeZone )
    {
        return Format.date( timeWritten, timeZone ) + "/" + timeWritten;
    }
}
