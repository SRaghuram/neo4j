/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;

import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

/**
 * Replicates commands to assign next available id range to this member.
 */
public class ReplicatedIdRangeAcquirer
{
    private final Replicator replicator;
    private final ReplicatedIdAllocationStateMachine idAllocationStateMachine;

    private final Map<IdType,Integer> allocationSizes;

    private final MemberId me;
    private final Log log;
    private final String databaseName;

    public ReplicatedIdRangeAcquirer( String databaseName,
            Replicator replicator, ReplicatedIdAllocationStateMachine idAllocationStateMachine,
            Map<IdType, Integer> allocationSizes, MemberId me, LogProvider logProvider )
    {
        this.replicator = replicator;
        this.idAllocationStateMachine = idAllocationStateMachine;
        this.allocationSizes = allocationSizes;
        this.me = me;
        this.log = logProvider.getLog( getClass() );
        this.databaseName = databaseName;
    }

    IdAllocation acquireIds( IdType idType )
    {
        while ( true )
        {
            long firstUnallocated = idAllocationStateMachine.firstUnallocated( idType );
            ReplicatedIdAllocationRequest idAllocationRequest =
                    new ReplicatedIdAllocationRequest( me, idType, firstUnallocated, allocationSizes.get( idType ), databaseName );

            if ( replicateIdAllocationRequest( idType, idAllocationRequest ) )
            {
                IdRange idRange = new IdRange( EMPTY_LONG_ARRAY, firstUnallocated, allocationSizes.get( idType ) );
                return new IdAllocation( idRange, -1, 0 );
            }
            else
            {
                log.info( "Retrying ID generation due to conflict. Request was: " + idAllocationRequest );
            }
        }
    }

    private boolean replicateIdAllocationRequest( IdType idType, ReplicatedIdAllocationRequest idAllocationRequest )
    {
        try
        {
            return (Boolean) replicator.replicate( idAllocationRequest ).consume();
        }
        catch ( Exception e )
        {
            log.warn( format( "Failed to acquire id range for idType %s", idType ), e );
            throw new IdGenerationException( e );
        }
    }
}
