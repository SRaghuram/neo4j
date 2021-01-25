/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;

/**
 * Represents a lease by an pairing a lease id together with its owner. A lease
 * is owned by a single member at any point in time and the lease holder is the
 * only one that should be performing leader only operations. The lease is used as
 * an ordering primitive in the consensus machinery for transactions.
 *
 * The reason for calling it a lease is to clarify the fact that there logically
 * is just a single valid lease at any point in time, which gets requested and
 * logically passed around. A transaction is only valid under a particular lease
 * and a transaction will be aborted if the lease changes before it has been
 * committed.
 */
interface Lease
{
    /**
     * Convenience method for retrieving a valid candidate id for a
     * lease request.
     *
     * @param currentId The current ID from which to generate the next.
     * @return A suitable candidate id for a token request.
     */
    static int nextCandidateId( int currentId )
    {
        int candidateId = currentId + 1;
        if ( candidateId == NO_LEASE )
        {
            candidateId++;
        }
        return candidateId;
    }

    /**
     * The id of the lease.
     */
    int id();

    /**
     * The owner of this lease.
     */
    Object owner();
}
