/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.utils;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;

public class Neo4jJsonCodec extends ObjectMapper
{

    @Override
    public void writeValue( JsonGenerator out, Object value ) throws IOException
    {
        if ( value instanceof PropertyContainer )
        {
            writePropertyContainer( out, (PropertyContainer) value );
        }
        else if ( value instanceof Path )
        {
            writePath( out, ((Path) value).iterator() );
        }
        else if ( value instanceof Iterable )
        {
            writeIterator( out, ((Iterable) value).iterator() );
        }
        else if ( value instanceof byte[] )
        {
            writeByteArray( out, (byte[]) value );
        }
        else if ( value instanceof Map )
        {
            writeMap( out, (Map) value );
        }
        else
        {
            super.writeValue( out, value );
        }
    }

    private void writeMap( JsonGenerator out, Map value ) throws IOException
    {
        out.writeStartObject();
        Set<Map.Entry> set = value.entrySet();
        for ( Map.Entry e : set )
        {
            out.writeFieldName( e.getKey().toString() );
            writeValue( out, e.getValue() );
        }
        out.writeEndObject();
    }

    private void writeIterator( JsonGenerator out, Iterator value ) throws IOException
    {
        out.writeStartArray();
        while ( value.hasNext() )
        {
            writeValue( out, value.next() );
        }
        out.writeEndArray();
    }

    private void writePath( JsonGenerator out, Iterator<PropertyContainer> value ) throws IOException
    {
        out.writeStartArray();
        while ( value.hasNext() )
        {
            writePropertyContainer( out, value.next() );
        }
        out.writeEndArray();
    }

    private void writePropertyContainer( JsonGenerator out, PropertyContainer value ) throws IOException
    {
        out.writeStartObject();
        for ( String key : value.getPropertyKeys() )
        {
            out.writeObjectField( key, value.getProperty( key ) );
        }
        out.writeEndObject();
    }

    private void writeByteArray( JsonGenerator out, byte[] bytes ) throws IOException
    {
        out.writeStartArray();
        for ( byte b : bytes )
        {
            out.writeNumber( (int) b );
        }
        out.writeEndArray();
    }
}
