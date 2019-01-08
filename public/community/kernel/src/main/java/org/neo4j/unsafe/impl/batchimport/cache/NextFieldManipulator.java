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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.neo4j.unsafe.impl.batchimport.Utils;

/**
 * Manipulates relationship group cache field containing NEXT and TYPE.
 */
public class NextFieldManipulator
{
    private static final LongBitsManipulator MANIPULATOR = new LongBitsManipulator( 64 - 16 /*next*/, 16 /*type*/ );
    private static final long EMPTY_FIELD = MANIPULATOR.template( true, false );

    private NextFieldManipulator()
    {
    }

    public static long setNext( long field, long next )
    {
        return MANIPULATOR.set( field, 0, next );
    }

    public static long getNext( long field )
    {
        return MANIPULATOR.get( field, 0 );
    }

    public static long setType( long field, int type )
    {
        return MANIPULATOR.set( field, 1, type );
    }

    public static int getType( long field )
    {
        return Utils.safeCastLongToInt( MANIPULATOR.get( field, 1 ) );
    }

    public static long initialFieldWithType( int type )
    {
        return setType( EMPTY_FIELD, type );
    }
}
