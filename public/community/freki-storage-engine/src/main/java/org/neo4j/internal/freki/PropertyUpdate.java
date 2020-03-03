/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.freki;

import java.nio.ByteBuffer;
import java.util.Objects;

class PropertyUpdate
{
    final FrekiCommand.Mode mode;
    final int propertyKeyId;
    final ByteBuffer before;
    final ByteBuffer after;

    PropertyUpdate( FrekiCommand.Mode mode, int propertyKeyId, ByteBuffer before, ByteBuffer after )
    {
        this.mode = mode;
        this.propertyKeyId = propertyKeyId;
        this.before = before;
        this.after = after;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        PropertyUpdate update = (PropertyUpdate) o;
        return propertyKeyId == update.propertyKeyId && mode == update.mode && Objects.equals( before, update.before ) && Objects.equals( after, update.after );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( mode, propertyKeyId, before, after );
    }

    static PropertyUpdate add( int propertyKeyId, ByteBuffer value )
    {
        return new PropertyUpdate( FrekiCommand.Mode.CREATE, propertyKeyId, null, value );
    }

    static PropertyUpdate change( int propertyKeyId, ByteBuffer valueBefore, ByteBuffer valueAfter )
    {
        return new PropertyUpdate( FrekiCommand.Mode.UPDATE, propertyKeyId, valueBefore, valueAfter );
    }

    static PropertyUpdate remove( int propertyKeyId, ByteBuffer value )
    {
        return new PropertyUpdate( FrekiCommand.Mode.DELETE, propertyKeyId, value, null );
    }
}
