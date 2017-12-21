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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.StringNew;

import org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew.Encoder;

public class ControlledEncoder implements Encoder
{
    private final Encoder actual;
    private Object overrideId;

    public ControlledEncoder( Encoder actual )
    {
        this.actual = actual;
    }

    /**
     * Single use in {@link #encode(Object)}.
     */
    public void useThisIdToEncodeNoMatterWhatComesIn( Object id )
    {
        this.overrideId = id;
    }

    @Override
    public long encode( Object value, int version )
    {
        try
        {
            return actual.encode( overrideId != null ? overrideId : value );
        }
        finally
        {
            overrideId = null;
        }
    }

    @Override
    public long encode( Object value )
    {
        // TODO Auto-generated method stub
        return encode( value, 0 );
    }
}
