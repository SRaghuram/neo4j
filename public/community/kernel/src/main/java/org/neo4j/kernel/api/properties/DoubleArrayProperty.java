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
package org.neo4j.kernel.api.properties;

class DoubleArrayProperty extends FloatingPointArrayProperty
{
    private final double[] value;

    DoubleArrayProperty( int propertyKeyId, double[] value )
    {
        super( propertyKeyId );
        assert value != null;
        this.value = value;
    }

    @Override
    public double[] value()
    {
        return value.clone();
    }

    @Override
    public int length()
    {
        return value.length;
    }

    @Override
    public double doubleValue( int index )
    {
        return value[index];
    }
}
