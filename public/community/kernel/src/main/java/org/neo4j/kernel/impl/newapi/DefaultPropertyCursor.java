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
package org.neo4j.kernel.impl.newapi;

import java.util.Iterator;
import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.newapi.Read.NO_ID;

public class DefaultPropertyCursor implements PropertyCursor
{
    private Read read;
    private StoragePropertyCursor storeCursor;
    private PropertyContainerState propertiesState;
    private Iterator<StorageProperty> txStateChangedProperties;
    private StorageProperty txStateValue;
    private AssertOpen assertOpen;
    private final CursorPool<DefaultPropertyCursor> pool;
    private AccessMode accessMode;
    private boolean checkReadProperty = false;
    private long entityReference = -1L;

    DefaultPropertyCursor( CursorPool<DefaultPropertyCursor> pool, StoragePropertyCursor storeCursor )
    {
        this.pool = pool;
        this.storeCursor = storeCursor;
    }

    void initNode( long nodeReference, long reference, Read read, AssertOpen assertOpen )
    {
        assert nodeReference != NO_ID;

        init( read, assertOpen );
        storeCursor.initNodeProperties( reference );
        checkReadProperty = true;
        this.entityReference = nodeReference;

        // Transaction state
        if ( read.hasTxStateWithChanges() )
        {
            this.propertiesState = read.txState().getNodeState( nodeReference );
            this.txStateChangedProperties = this.propertiesState.addedAndChangedProperties();
        }
        else
        {
            this.propertiesState = null;
            this.txStateChangedProperties = null;
        }
    }

    void initRelationship( long relationshipReference, long reference, Read read, AssertOpen assertOpen )
    {
        assert relationshipReference != NO_ID;

        init( read, assertOpen );
        storeCursor.initRelationshipProperties( reference );
        checkReadProperty = false;
        entityReference = relationshipReference;

        // Transaction state
        if ( read.hasTxStateWithChanges() )
        {
            this.propertiesState = read.txState().getRelationshipState( relationshipReference );
            this.txStateChangedProperties = this.propertiesState.addedAndChangedProperties();
        }
        else
        {
            this.propertiesState = null;
            this.txStateChangedProperties = null;
        }
    }

    void initGraph( Read read, AssertOpen assertOpen )
    {
        init( read, assertOpen );
        storeCursor.initGraphProperties();
        checkReadProperty = false;
        entityReference = -1L;

        // Transaction state
        if ( read.hasTxStateWithChanges() )
        {
            this.propertiesState = read.txState().getGraphState( );
            if ( this.propertiesState != null )
            {
                this.txStateChangedProperties = this.propertiesState.addedAndChangedProperties();
            }
            else
            {
                this.txStateChangedProperties = null;
            }
        }
        else
        {
            this.txStateChangedProperties = null;
            this.propertiesState = null;
        }
    }

    private void init( Read read, AssertOpen assertOpen )
    {
        this.assertOpen = assertOpen;
        this.read = read;
        this.accessMode = read.ktx.securityContext().mode();
    }

    boolean allowedForNode( int propertyKey )
    {
        if ( checkReadProperty )
        {
            return accessMode.allowsPropertyReads( propertyKey ) &&
                   accessMode.allowsReadProperty( new NodeLabels( entityReference, read ), propertyKey );
        }
        else
        {
            return true;
        }
    }

    @Override
    public boolean next()
    {
        if ( txStateChangedProperties != null )
        {
            if ( txStateChangedProperties.hasNext() )
            {
                txStateValue = txStateChangedProperties.next();
                return true;
            }
            else
            {
                txStateChangedProperties = null;
                txStateValue = null;
            }
        }

        while ( storeCursor.next() )
        {
            boolean skip = propertiesState != null && propertiesState.isPropertyChangedOrRemoved( storeCursor.propertyKey() );
            if ( !skip && allowedForNode( propertyKey() ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            propertiesState = null;
            txStateChangedProperties = null;
            txStateValue = null;
            read = null;
            storeCursor.reset();
            accessMode = null;

            pool.accept( this );
        }
    }

    @Override
    public int propertyKey()
    {
        if ( txStateValue != null )
        {
            return txStateValue.propertyKeyId();
        }
        return storeCursor.propertyKey();
    }

    @Override
    public ValueGroup propertyType()
    {
        if ( txStateValue != null )
        {
            return txStateValue.value().valueGroup();
        }
        return storeCursor.propertyType();
    }

    @Override
    public Value propertyValue()
    {
        if ( txStateValue != null )
        {
            return txStateValue.value();
        }

        Value value = storeCursor.propertyValue();

        assertOpen.assertOpen();
        return value;
    }

    @Override
    public boolean isClosed()
    {
        return read == null;
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "PropertyCursor[closed state]";
        }
        else
        {
            return "PropertyCursor[id=" + propertyKey() +
                   ", " + storeCursor.toString() + " ]";
        }
    }

    public void release()
    {
        storeCursor.close();
    }

    private class NodeLabels implements Supplier<LabelSet>
    {
        private final long nodeReference;
        private final Read read;
        private LabelSet labels;

        private NodeLabels( long nodeReference, Read read )
        {
            this.nodeReference = nodeReference;
            this.read = read;
        }

        @Override
        public LabelSet get()
        {
            if ( labels == null )
            {
                try ( NodeCursor nodeCursor = read.cursors().allocateFullAccessNodeCursor() )
                {
                    read.singleNode( nodeReference, nodeCursor );
                    nodeCursor.next();
                    labels = nodeCursor.labels();
                }
            }
            return labels;
        }
    }
}
