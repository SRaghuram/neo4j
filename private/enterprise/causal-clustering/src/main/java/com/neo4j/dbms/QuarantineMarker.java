/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.SafeStateMarshal;

import static java.util.Objects.requireNonNull;

public class QuarantineMarker
{
    private final String message;

    QuarantineMarker( String message )
    {
        this.message = requireNonNull( message );
    }

    public String message()
    {
        return message;
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
        QuarantineMarker that = (QuarantineMarker) o;
        return Objects.equals( message, that.message );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( message );
    }

    public static class Marshal extends SafeStateMarshal<QuarantineMarker>
    {
        public static final QuarantineMarker.Marshal INSTANCE = new QuarantineMarker.Marshal();

        @Override
        public void marshal( QuarantineMarker marker, WritableChannel channel ) throws IOException
        {
            var message = marker.message();
            StringMarshal.marshal( channel, message );
        }

        @Override
        public QuarantineMarker unmarshal0( ReadableChannel channel ) throws IOException
        {
            var message = StringMarshal.unmarshal( channel );
            return new QuarantineMarker( message );
        }

        @Override
        public QuarantineMarker startState()
        {
            return null;
        }

        @Override
        public long ordinal( QuarantineMarker marker )
        {
            return marker == null ? 0 : 1;
        }
    }
}
