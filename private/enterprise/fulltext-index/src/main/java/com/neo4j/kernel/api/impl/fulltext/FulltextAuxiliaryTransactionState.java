/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.StorageCommand;

/**
 * The fulltext auxiliary transaction state manages the aggregate transaction state of <em>all</em> fulltext indexes in a transaction.
 * <p>
 * For the transaction state of the individual fulltext schema index, see the {@link FulltextIndexTransactionState} class.
 */
class FulltextAuxiliaryTransactionState implements AuxiliaryTransactionState, Function<IndexReference,FulltextIndexTransactionState>
{
    private final FulltextIndexProvider provider;
    private final Log log;
    private final Map<IndexReference,FulltextIndexTransactionState> indexStates;

    FulltextAuxiliaryTransactionState( FulltextIndexProvider provider, Log log )
    {
        this.provider = provider;
        this.log = log;
        indexStates = new HashMap<>();
    }

    @Override
    public void close() throws Exception
    {
        IOUtils.closeAll( indexStates.values() );
    }

    @Override
    public boolean hasChanges()
    {
        // We always return 'false' here, because we only use this transaction state for reading.
        //Our index changes are already derived from the store commands, so we never have any commands of our own to extract.
        return false;
    }

    @Override
    public void extractCommands( Collection<StorageCommand> target )
    {
        // We never have any commands to extract, because this transaction state is only used for reading.
    }

    FulltextIndexReader indexReader( IndexReference indexReference, KernelTransactionImplementation kti )
    {
        FulltextIndexTransactionState state = indexStates.computeIfAbsent( indexReference, this );
        return state.getIndexReader( kti );
    }

    @Override
    public FulltextIndexTransactionState apply( IndexReference indexReference )
    {
        return new FulltextIndexTransactionState( provider, log, indexReference );
    }
}
