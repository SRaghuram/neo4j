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
package org.neo4j.internal.tokenstore;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

class TokenLayout extends Layout.Adapter<TokenKey,Void>
{
    private static final int MAX_ARRAY_LENGTH = Short.MAX_VALUE;

    TokenLayout()
    {
        super( false, 5383432312L, 0, 0 );
    }

    @Override
    public TokenKey newKey()
    {
        return new TokenKey( 0 );
    }

    @Override
    public TokenKey copyKey( TokenKey key, TokenKey into )
    {
        into.id = key.id;
        into.nameBytes = key.nameBytes.clone();
        return into;
    }

    @Override
    public Void newValue()
    {
        return null;
    }

    @Override
    public int keySize( TokenKey tokenKey )
    {
        return Integer.BYTES +            // id
               Integer.BYTES +            // name length
               tokenKey.nameBytes.length; // name
    }

    @Override
    public int valueSize( Void tokenValue )
    {
        return 0;
    }

    @Override
    public void writeKey( PageCursor cursor, TokenKey key )
    {
        cursor.putInt( key.id );
        cursor.putInt( key.nameBytes.length );
        cursor.putBytes( key.nameBytes );
    }

    @Override
    public void writeValue( PageCursor cursor, Void tokenValue )
    {
    }

    @Override
    public void readKey( PageCursor cursor, TokenKey into, int keySize )
    {
        into.id = cursor.getInt();
        int length = cursor.getInt();
        if ( length < 0 || length > MAX_ARRAY_LENGTH )
        {
            cursor.setCursorException( "Unreasonable name length " + length );
            return;
        }
        into.nameBytes = new byte[length];
        cursor.getBytes( into.nameBytes );
    }

    @Override
    public void readValue( PageCursor cursor, Void into, int valueSize )
    {
    }

    @Override
    public void initializeAsLowest( TokenKey key )
    {
        key.id = Integer.MIN_VALUE;
    }

    @Override
    public void initializeAsHighest( TokenKey key )
    {
        key.id = Integer.MAX_VALUE;
    }

    @Override
    public int compare( TokenKey o1, TokenKey o2 )
    {
        return Integer.compare( o1.id, o2.id );
    }
}
