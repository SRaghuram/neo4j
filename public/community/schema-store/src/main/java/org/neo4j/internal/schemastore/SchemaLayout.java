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
package org.neo4j.internal.schemastore;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.values.storable.UTF8StringValue.byteArrayCompare;

class SchemaLayout extends Layout.Adapter<SchemaKey,SchemaValue>
{
    private static final int ARRAY_LENGTH_MAX_LENGTH = Short.MAX_VALUE;

    SchemaLayout()
    {
        super( false, 5383312L, 0, 0 );
    }

    @Override
    public SchemaKey newKey()
    {
        return new SchemaKey( 0 );
    }

    @Override
    public SchemaKey copyKey( SchemaKey key, SchemaKey into )
    {
        into.id = key.id;
        into.nameBytes = key.nameBytes.clone();
        return into;
    }

    @Override
    public SchemaValue newValue()
    {
        return new SchemaValue();
    }

    @Override
    public int keySize( SchemaKey key )
    {
        return Long.BYTES + Integer.BYTES + key.nameBytes.length;
    }

    @Override
    public int valueSize( SchemaValue value )
    {
        return value.size();
    }

    @Override
    public void writeKey( PageCursor cursor, SchemaKey key )
    {
        cursor.putLong( key.id );
        cursor.putInt( key.nameBytes.length );
        cursor.putBytes( key.nameBytes );
    }

    @Override
    public void writeValue( PageCursor cursor, SchemaValue value )
    {
        value.write( cursor );
    }

    @Override
    public void readKey( PageCursor cursor, SchemaKey into, int keySize )
    {
        into.id = cursor.getLong();
        int length = cursor.getInt();
        if ( !checkArrayLengthSanity( cursor, length ) )
        {
            return;
        }
        into.nameBytes = new byte[length];
        cursor.getBytes( into.nameBytes );
    }

    @Override
    public void readValue( PageCursor cursor, SchemaValue into, int valueSize )
    {
        into.read( cursor, valueSize );
    }

    @Override
    public void initializeAsLowest( SchemaKey key )
    {
        key.id = Long.MIN_VALUE;
        key.nameBytes = null;
    }

    @Override
    public void initializeAsHighest( SchemaKey key )
    {
        key.id = Long.MAX_VALUE;
        key.nameBytes = null;
    }

    @Override
    public int compare( SchemaKey o1, SchemaKey o2 )
    {
        int idComparison = Long.compare( o1.id, o2.id );
        if ( idComparison != 0 )
        {
            return idComparison;
        }
        if ( o1.nameBytes != null && o2.nameBytes != null )
        {
            return byteArrayCompare( o1.nameBytes, o2.nameBytes );
        }
        return Boolean.compare( o1.nameBytes != null, o2.nameBytes != null );
    }

    static boolean checkArrayLengthSanity( PageCursor cursor, int length )
    {
        if ( length < 0 || length > ARRAY_LENGTH_MAX_LENGTH )
        {
            cursor.setCursorException( "Unreasonable length " + length );
            return false;
        }
        return true;
    }
}
