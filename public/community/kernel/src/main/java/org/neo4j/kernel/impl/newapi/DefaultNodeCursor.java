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

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.NodeState;

import static org.neo4j.kernel.impl.newapi.Read.NO_ID;
import static org.neo4j.storageengine.api.LongReference.NULL_REFERENCE;

class DefaultNodeCursor extends TraceableCursor implements NodeCursor
{
    Read read;
    HasChanges hasChanges = HasChanges.MAYBE;
    private LongIterator addedNodes;
    StorageNodeCursor storeCursor;
    private long currentAddedInTx;
    private long single;
    private AccessMode accessMode;

    private final CursorPool<DefaultNodeCursor> pool;

    DefaultNodeCursor( CursorPool<DefaultNodeCursor> pool, StorageNodeCursor storeCursor )
    {
        this.pool = pool;
        this.storeCursor = storeCursor;
    }

    void scan( Read read )
    {
        storeCursor.scan();
        this.read = read;
        this.single = NO_ID;
        this.currentAddedInTx = NO_ID;
        this.hasChanges = HasChanges.MAYBE;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
        if ( tracer != null )
        {
            tracer.onAllNodesScan();
        }
    }

    boolean scanBatch( Read read, AllNodeScan scan, int sizeHint, LongIterator addedNodes, boolean hasChanges )
    {
        this.read = read;
        this.single = NO_ID;
        this.currentAddedInTx = NO_ID;
        this.hasChanges = hasChanges ? HasChanges.YES : HasChanges.NO;
        this.addedNodes = addedNodes;
        boolean scanBatch = storeCursor.scanBatch( scan, sizeHint );
        return addedNodes.hasNext() || scanBatch;
    }

    void single( long reference, Read read )
    {
        storeCursor.single( reference );
        this.read = read;
        this.single = reference;
        this.currentAddedInTx = NO_ID;
        this.hasChanges = HasChanges.MAYBE;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
    }

    protected boolean currentNodeIsAddedInTx()
    {
        return currentAddedInTx != NO_ID;
    }

    @Override
    public long nodeReference()
    {
        if ( currentAddedInTx != NO_ID )
        {
            // Special case where the most recent next() call selected a node that exists only in tx-state.
            // Internal methods getting data about this node will also check tx-state and get the data from there.
            return currentAddedInTx;
        }
        return storeCursor.entityReference();
    }

    @Override
    public TokenSet labels()
    {
        if ( currentAddedInTx != NO_ID )
        {
            //Node added in tx-state, no reason to go down to store and check
            TransactionState txState = read.txState();
            return Labels.from( txState.nodeStateLabelDiffSets( currentAddedInTx ).getAdded() );
        }
        else if ( hasChanges() )
        {
            //Get labels from store and put in intSet, unfortunately we get longs back
            TransactionState txState = read.txState();
            long[] longs = storeCursor.labels();
            final MutableLongSet labels = new LongHashSet();
            for ( long labelToken : longs )
            {
                labels.add( labelToken );
            }

            //Augment what was found in store with what we have in tx state
            return Labels.from( txState.augmentLabels( labels, txState.getNodeState( storeCursor.entityReference() ) ) );
        }
        else
        {
            //Nothing in tx state, just read the data.
            return Labels.from( storeCursor.labels() );
        }
    }

    /**
     * The normal labels() method takes into account TxState for both created nodes and set/remove labels.
     * Some code paths need to consider created, but not changed labels.
     */
    @Override
    public TokenSet labelsIgnoringTxStateSetRemove()
    {
        if ( currentAddedInTx != NO_ID )
        {
            //Node added in tx-state, no reason to go down to store and check
            TransactionState txState = read.txState();
            return Labels.from( txState.nodeStateLabelDiffSets( currentAddedInTx ).getAdded() );
        }
        else
        {
            //Nothing in tx state, just read the data.
            return Labels.from( storeCursor.labels() );
        }
    }

    @Override
    public boolean hasLabel( int label )
    {
        if ( hasChanges() )
        {
            TransactionState txState = read.txState();
            LongDiffSets diffSets = txState.nodeStateLabelDiffSets( nodeReference() );
            if ( diffSets.getAdded().contains( label ) )
            {
                return true;
            }
            if ( diffSets.getRemoved().contains( label ) || currentAddedInTx != NO_ID )
            {
                return false;
            }
        }

        //Get labels from store and put in intSet, unfortunately we get longs back
        return storeCursor.hasLabel( label );
    }

    @Override
    public void relationships( RelationshipTraversalCursor cursor, RelationshipSelection selection )
    {
        ((DefaultRelationshipTraversalCursor) cursor).init( this, selection, read );
    }

    @Override
    public boolean relationshipsTo( RelationshipTraversalCursor relationships, RelationshipSelection selection, long neighbourNodeReference )
    {
        return ((DefaultRelationshipTraversalCursor) relationships).init( this, selection, neighbourNodeReference, read );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initNode( this, read, read );
    }

    @Override
    public long relationshipsReference()
    {
        return currentAddedInTx != NO_ID ? NO_ID : storeCursor.relationshipsReference();
    }

    @Override
    public Reference propertiesReference()
    {
        return currentAddedInTx != NO_ID ? NULL_REFERENCE : storeCursor.propertiesReference();
    }

    @Override
    public boolean supportsFastDegreeLookup()
    {
        return currentAddedInTx == NO_ID && storeCursor.supportsFastDegreeLookup();
    }

    @Override
    public int[] relationshipTypes()
    {
        boolean hasChanges = hasChanges();
        NodeState nodeTxState = hasChanges ? read.txState().getNodeState( nodeReference() ) : null;
        int[] storedTypes = currentAddedInTx == NO_ID ? storeCursor.relationshipTypes() : null;
        MutableIntSet types = storedTypes != null ? IntSets.mutable.of( storedTypes ) : IntSets.mutable.empty();
        if ( nodeTxState != null )
        {
            types.addAll( nodeTxState.getAddedRelationshipTypes() );
        }
        return types.toArray();
    }

    @Override
    public Degrees degrees( RelationshipSelection selection )
    {
        boolean hasChanges = hasChanges();
        NodeState nodeTxState = hasChanges ? read.txState().getNodeState( nodeReference() ) : null;
        Degrees storedDegrees = currentAddedInTx == NO_ID ? storeCursor.degrees( selection ) : null;
        if ( nodeTxState == null )
        {
            return storedDegrees == null ? Degrees.EMPTY : storedDegrees;
        }

        IntIterable txTypes = nodeTxState.getAddedRelationshipTypes();
        if ( storedDegrees == null )
        {
            return new Degrees()
            {
                @Override
                public int[] types()
                {
                    return txTypes.toArray();
                }

                @Override
                public int degree( int type, Direction direction )
                {
                    return nodeTxState.augmentDegree( direction, 0, type );
                }
            };
        }
        return new Degrees()
        {
            @Override
            public int[] types()
            {
                MutableIntSet allTypes = IntSets.mutable.of( storedDegrees.types() );
                allTypes.addAll( txTypes.toArray() );
                return allTypes.toArray();
            }

            @Override
            public int degree( int type, Direction direction )
            {
                int storedDegree = storedDegrees.degree( type, direction );
                return nodeTxState.augmentDegree( direction, storedDegree, type );
            }
        };
    }

    @Override
    public boolean next()
    {
        // Check tx state
        boolean hasChanges = hasChanges();

        if ( hasChanges )
        {
            if ( addedNodes.hasNext() )
            {
                currentAddedInTx = addedNodes.next();
                if ( tracer != null )
                {
                    tracer.onNode( nodeReference() );
                }
                return true;
            }
            else
            {
                currentAddedInTx = NO_ID;
            }
        }

        while ( storeCursor.next() )
        {
            boolean skip = hasChanges && read.txState().nodeIsDeletedInThisTx( storeCursor.entityReference() );
            if ( !skip && allowsTraverse() )
            {
                if ( tracer != null )
                {
                    tracer.onNode( nodeReference() );
                }
                return true;
            }
        }
        return false;
    }

    boolean allowsTraverse()
    {
        if ( accessMode == null )
        {
            accessMode = read.ktx.securityContext().mode();
        }
        return accessMode.allowsTraverseAllLabels() || accessMode.allowsTraverseNode( storeCursor.labels() );
    }

    @Override
    public void closeInternal()
    {
        if ( !isClosed() )
        {
            read = null;
            hasChanges = HasChanges.MAYBE;
            addedNodes = ImmutableEmptyLongIterator.INSTANCE;
            storeCursor.reset();
            accessMode = null;

            pool.accept( this );
        }
    }

    @Override
    public boolean isClosed()
    {
        return read == null;
    }

    /**
     * NodeCursor should only see changes that are there from the beginning
     * otherwise it will not be stable.
     */
    boolean hasChanges()
    {
        switch ( hasChanges )
        {
        case MAYBE:
            boolean changes = read.hasTxStateWithChanges();
            if ( changes )
            {
                if ( single != NO_ID )
                {
                    addedNodes = read.txState().nodeIsAddedInThisTx( single ) ?
                                 LongSets.immutable.of( single ).longIterator() : ImmutableEmptyLongIterator.INSTANCE;
                }
                else
                {
                    addedNodes = read.txState().addedAndRemovedNodes().getAdded().freeze().longIterator();
                }
                hasChanges = HasChanges.YES;
            }
            else
            {
                hasChanges = HasChanges.NO;
            }
            return changes;
        case YES:
            return true;
        case NO:
            return false;
        default:
            throw new IllegalStateException( "Style guide, why are you making me do this" );
        }
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "NodeCursor[closed state]";
        }
        else
        {
            return "NodeCursor[id=" + nodeReference() + ", " + storeCursor.toString() + "]";
        }
    }

    void release()
    {
        storeCursor.close();
    }
}
