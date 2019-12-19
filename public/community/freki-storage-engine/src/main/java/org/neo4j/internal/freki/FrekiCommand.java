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
package org.neo4j.internal.freki;

import java.io.IOException;

import org.neo4j.io.fs.WritableChannel;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.string.UTF8;
import org.neo4j.token.api.NamedToken;

abstract class FrekiCommand implements StorageCommand
{
    private final byte recordType;

    FrekiCommand( byte recordType )
    {
        this.recordType = recordType;
    }

    @Override
    public void serialize( WritableChannel channel ) throws IOException
    {
        channel.put( recordType );
    }

    abstract boolean accept( TransactionApplier applier ) throws IOException;

    static abstract class FrekiRecordCommand extends FrekiCommand
    {
        private final Record before;
        private final Record after;

        FrekiRecordCommand( byte recordType, Record before, Record after )
        {
            super( recordType );
            this.before = before;
            this.after = after;
        }

        Record before()
        {
            return before;
        }

        Record after()
        {
            return after;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            before.serialize( channel );
            after.serialize( channel );
        }
    }

    static class Node extends FrekiRecordCommand
    {
        static final byte TYPE = 0;

        Node( Record before, Record after )
        {
            super( TYPE, before, after );
        }

        @Override
        boolean accept( TransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }
    }

    static abstract class Token extends FrekiCommand
    {
        final NamedToken token;

        Token( byte recordType, NamedToken token )
        {
            super( recordType );
            this.token = token;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            channel.putInt( token.id() );
            byte[] name = UTF8.encode( token.name() );
            channel.putInt( name.length );
            channel.put( name, name.length );
        }
    }

    static class LabelToken extends Token
    {
        static final byte TYPE = 1;

        LabelToken( NamedToken token )
        {
            super( TYPE, token );
        }

        @Override
        boolean accept( TransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }
    }

    static class RelationshipTypeToken extends Token
    {
        static final byte TYPE = 1;

        RelationshipTypeToken( NamedToken token )
        {
            super( TYPE, token );
        }

        @Override
        boolean accept( TransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }
    }

    static class PropertyKeyToken extends Token
    {
        static final byte TYPE = 1;

        PropertyKeyToken( NamedToken token )
        {
            super( TYPE, token );
        }

        @Override
        boolean accept( TransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }
    }

    interface Dispatcher
    {
        void handle( Node node ) throws IOException;

        void handle( LabelToken token ) throws IOException;

        void handle( RelationshipTypeToken token ) throws IOException;

        void handle( PropertyKeyToken token ) throws IOException;

        class Adapter implements Dispatcher
        {
            @Override
            public void handle( Node node ) throws IOException
            {
            }

            @Override
            public void handle( LabelToken token ) throws IOException
            {
            }

            @Override
            public void handle( RelationshipTypeToken token ) throws IOException
            {
            }

            @Override
            public void handle( PropertyKeyToken token ) throws IOException
            {
            }
        }
    }
}
