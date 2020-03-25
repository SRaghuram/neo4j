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
package org.neo4j.kernel.impl.newapi;

import java.util.Iterator;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.txstate.EntityState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.newapi.Read.NO_ID;
import static org.neo4j.token.api.TokenConstants.NO_TOKEN;
import static org.neo4j.values.storable.Values.NO_VALUE;

public class DefaultPropertyCursor extends TraceableCursor implements PropertyCursor, Supplier<TokenSet>, IntSupplier
{
    private static final int NODE = -2;
    private Read read;
    private StoragePropertyCursor storeCursor;
    private final PageCursorTracer cursorTracer;
    private EntityState propertiesState;
    private Iterator<StorageProperty> txStateChangedProperties;
    private StorageProperty txStateValue;
    private AssertOpen assertOpen;
    private final CursorPool<DefaultPropertyCursor> pool;
    private AccessMode accessMode;
    private long entityReference = NO_ID;
    private TokenSet labels;
    //stores relationship type or NODE if not a relationship
    private int type = NO_TOKEN;
    private boolean addedInTx;

    DefaultPropertyCursor( CursorPool<DefaultPropertyCursor> pool, StoragePropertyCursor storeCursor, PageCursorTracer cursorTracer )
    {
        this.pool = pool;
        this.storeCursor = storeCursor;
        this.cursorTracer = cursorTracer;
    }

    void initNode( long nodeReference, Reference reference, Read read, AssertOpen assertOpen )
    {
        assert nodeReference != NO_ID;

        init( read, assertOpen );
        this.type = NODE;
        storeCursor.initNodeProperties( reference );
        this.entityReference = nodeReference;

        initializeNodeTransactionState( nodeReference, read );
    }

    void initNode( DefaultNodeCursor nodeCursor, Read read, AssertOpen assertOpen )
    {
        entityReference = nodeCursor.nodeReference();
        assert entityReference != NO_ID;

        init( read, assertOpen );
        this.type = NODE;
        addedInTx = nodeCursor.currentNodeIsAddedInTx();
        if ( !addedInTx )
        {
            storeCursor.initNodeProperties( nodeCursor.storeCursor );
        }

        initializeNodeTransactionState( entityReference, read );
    }

    private void initializeNodeTransactionState( long nodeReference, Read read )
    {
        if ( read.hasTxStateWithChanges() )
        {
            this.propertiesState = read.txState().getNodeState( nodeReference );
            this.txStateChangedProperties = this.propertiesState.addedAndChangedProperties().iterator();
        }
        else
        {
            this.propertiesState = null;
            this.txStateChangedProperties = null;
        }
    }

    void initRelationship( long relationshipReference, Reference reference, Read read, AssertOpen assertOpen )
    {
        assert relationshipReference != NO_ID;

        init( read, assertOpen );
        storeCursor.initRelationshipProperties( reference );
        this.entityReference = relationshipReference;

        initializeRelationshipTransactionState( relationshipReference, read );
    }

    void initRelationship( DefaultRelationshipCursor<?> relationshipCursor, Read read, AssertOpen assertOpen )
    {
        entityReference = relationshipCursor.relationshipReference();
        assert entityReference != NO_ID;

        init( read, assertOpen );
        addedInTx = relationshipCursor.currentRelationshipIsAddedInTx();
        if ( !addedInTx )
        {
            storeCursor.initRelationshipProperties( relationshipCursor.storeCursor );
        }

        initializeRelationshipTransactionState( entityReference, read );
    }

    private void initializeRelationshipTransactionState( long relationshipReference, Read read )
    {
        // Transaction state
        if ( read.hasTxStateWithChanges() )
        {
            this.propertiesState = read.txState().getRelationshipState( relationshipReference );
            this.txStateChangedProperties = this.propertiesState.addedAndChangedProperties().iterator();
        }
        else
        {
            this.propertiesState = null;
            this.txStateChangedProperties = null;
        }
    }

    private void init( Read read, AssertOpen assertOpen )
    {
        this.assertOpen = assertOpen;
        this.read = read;
        this.labels = null;
        this.type = NO_TOKEN;
        this.addedInTx = false;
    }

    boolean allowed()
    {
        if ( isNode() )
        {
            ensureAccessMode();
            return accessMode.allowsReadNodeProperty( this, propertyKey() );
        }
        else
        {
            ensureAccessMode();
            return accessMode.allowsReadRelationshipProperty( this, propertyKey() );
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
                if ( tracer != null )
                {
                    tracer.onProperty( propertyKey() );
                }
                return true;
            }
            else
            {
                txStateChangedProperties = null;
                txStateValue = null;
            }
        }
        if ( addedInTx )
        {
            return false;
        }

        while ( storeCursor.next() )
        {
            boolean skip = propertiesState != null && propertiesState.isPropertyChangedOrRemoved( storeCursor.propertyKey() );
            if ( !skip && allowed( ) )
            {
                if ( tracer != null )
                {
                    tracer.onProperty( propertyKey() );
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public void closeInternal()
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
    public boolean seekProperty( int property )
    {
        if ( property == NO_TOKEN  )
        {
            return false;
        }
        boolean found = seekToProperty( property );
        storeCursor.reset();
        return found;
    }

    @Override
    public Value seekPropertyValue( int property )
    {
        if ( property != NO_TOKEN  )
        {
            if ( seekToProperty( property ) )
            {
                Value value = propertyValue();
                storeCursor.reset();
                return value;
            }
        }
        return NO_VALUE;
    }

    private boolean seekToProperty( int property )
    {
        while ( next() )
        {
            if ( property == this.propertyKey() )
            {
                return true;
            }
        }
        return false;
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

    /**
     * Gets the label while ignoring removes in the tx state. Implemented as a Supplier so that we don't need additional
     * allocations.
     */
    @Override
    public TokenSet get()
    {
        assert isNode();

        if ( labels == null )
        {
            try ( NodeCursor nodeCursor = read.cursors().allocateFullAccessNodeCursor( cursorTracer ) )
            {
                read.singleNode( entityReference, nodeCursor );
                nodeCursor.next();
                labels = nodeCursor.labelsIgnoringTxStateSetRemove();
            }
        }
        return labels;
    }

    @Override
    public int getAsInt()
    {
        assert isRelationship();

        if ( type < 0 )
        {
            try ( RelationshipScanCursor relCursor = read.cursors()
                    .allocateFullAccessRelationshipScanCursor( cursorTracer ) )
            {
                read.singleRelationship( entityReference, relCursor );
                relCursor.next();
                this.type = relCursor.type();
            }
        }
        return type;
    }

    private void ensureAccessMode()
    {
        if ( accessMode == null )
        {
            accessMode = read.ktx.securityContext().mode();
        }
    }

    public void release()
    {
        storeCursor.close();
    }

    private boolean isNode()
    {
        return type == NODE;
    }

    private boolean isRelationship()
    {
        return type != NODE;
    }
}
