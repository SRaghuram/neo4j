/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.v3.messaging;

import java.util.Map;
import java.util.Objects;

import org.neo4j.bolt.v1.messaging.request.InitMessage;

public class HelloMessage extends InitMessage
{
    public static final byte SIGNATURE = InitMessage.SIGNATURE;
    private static final String USER_AGENT = "user_agent";
    private final Map<String,Object> meta;

    public HelloMessage( Map<String,Object> meta )
    {
        super( (String) meta.get( USER_AGENT ), meta );
        this.meta = meta;
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
        HelloMessage that = (HelloMessage) o;
        return Objects.equals( meta, that.meta );
    }

    public Map<String,Object> meta()
    {
        return meta;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( meta );
    }

    @Override
    public String toString()
    {
        return "HELLO " + meta;
    }
}
