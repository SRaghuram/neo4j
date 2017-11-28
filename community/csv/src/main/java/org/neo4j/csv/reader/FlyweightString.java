/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.csv.reader;

public interface FlyweightString
{
    int length();

    char charAt( int index );

    String asString();

    static FlyweightString wrap( Object object )
    {
        if ( object instanceof FlyweightString )
        {
            return (FlyweightString) object;
        }
        if ( object instanceof String )
        {
            return wrap( (String) object );
        }
        throw new IllegalArgumentException( "Unexpected " + object );
    }

    static FlyweightString wrap( String string )
    {
        return new FlyweightString()
        {
            @Override
            public int length()
            {
                return string.length();
            }

            @Override
            public char charAt( int index )
            {
                return string.charAt( index );
            }

            @Override
            public String asString()
            {
                return string;
            }
        };
    }
}
