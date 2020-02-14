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
package org.neo4j.internal.freki;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;

import java.io.IOException;

import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.string.UTF8;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.Value;

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

    abstract boolean accept( FrekiTransactionApplier applier ) throws IOException;

    abstract static class FrekiRecordCommand extends FrekiCommand
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

    static class SparseNode extends FrekiRecordCommand
    {
        static final byte TYPE = 1;

        SparseNode( Record before, Record after )
        {
            super( TYPE, before, after );
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }
    }

    static class DenseNode extends FrekiCommand
    {
        static final byte TYPE = 2;

        final long nodeId;
        final boolean inUse;
        final MutableIntObjectMap<Value> addedProperties;
        final MutableIntSet removedProperties;
        final MutableIntObjectMap<MutableNodeRecordData.Relationships> createdRelationships;
        final MutableIntObjectMap<MutableNodeRecordData.Relationships> deletedRelationships;

        DenseNode( long nodeId, boolean inUse, MutableIntObjectMap<Value> addedProperties, MutableIntSet removedProperties,
                MutableIntObjectMap<MutableNodeRecordData.Relationships> createdRelationships,
                MutableIntObjectMap<MutableNodeRecordData.Relationships> deletedRelationships )
        {
            super( TYPE );
            this.nodeId = nodeId;
            this.inUse = inUse;
            this.addedProperties = addedProperties;
            this.removedProperties = removedProperties;
            this.createdRelationships = createdRelationships;
            this.deletedRelationships = deletedRelationships;
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }
    }

    abstract static class Token extends FrekiCommand
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
        static final byte TYPE = 10;

        LabelToken( NamedToken token )
        {
            super( TYPE, token );
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }
    }

    static class RelationshipTypeToken extends Token
    {
        static final byte TYPE = 11;

        RelationshipTypeToken( NamedToken token )
        {
            super( TYPE, token );
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }
    }

    static class PropertyKeyToken extends Token
    {
        static final byte TYPE = 12;

        PropertyKeyToken( NamedToken token )
        {
            super( TYPE, token );
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }
    }

    enum Mode
    {
        CREATE,
        UPDATE,
        DELETE;
    }

    static class Schema extends FrekiCommand
    {
        static final byte TYPE = 13;

        final SchemaRule descriptor;
        final Mode mode;

        Schema( SchemaRule descriptor, Mode mode )
        {
            super( TYPE );
            this.descriptor = descriptor;
            this.mode = mode;
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }
    }

    interface Dispatcher
    {
        void handle( SparseNode node ) throws IOException;

        void handle( DenseNode node ) throws IOException;

        void handle( LabelToken token ) throws IOException;

        void handle( RelationshipTypeToken token ) throws IOException;

        void handle( PropertyKeyToken token ) throws IOException;

        void handle( Schema schema ) throws IOException;

        class Adapter implements Dispatcher
        {
            @Override
            public void handle( SparseNode node ) throws IOException
            {
            }

            @Override
            public void handle( DenseNode node ) throws IOException
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

            @Override
            public void handle( Schema schema ) throws IOException
            {
            }
        }
    }
}
