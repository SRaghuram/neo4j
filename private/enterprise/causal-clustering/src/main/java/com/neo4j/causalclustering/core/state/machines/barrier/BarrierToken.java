/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

/**
 * Represents a barrier token by an pairing an id together with its owner. A barrier token
 * is held by a single server at any point in time and the token holder is the
 * only one that should be performing leader only operations. It is used as an ordering primitive
 * in the consensus machinery to mark local lock validity by using it as
 * the cluster leader session id.
 *
 * The reason for calling it a token is to clarify the fact that there logically
 * is just a single valid token at any point in time, which gets requested and
 * logically passed around. When bound to a transaction the id gets used as a
 * barrier session id in the cluster.
 */
interface BarrierToken
{
    int INVALID_BARRIER_TOKEN_ID = -1;

    /**
     * Convenience method for retrieving a valid candidate id for a
     * barrier token request.
     *
     *  @return A suitable candidate id for a token request.
     * @param currentId
     */
    static int nextCandidateId( int currentId )
    {
        int candidateId = currentId + 1;
        if ( candidateId == INVALID_BARRIER_TOKEN_ID )
        {
            candidateId++;
        }
        return candidateId;
    }

    /**
     * The id of the barrier token.
     */
    int id();

    /**
     * The owner of this barrier token.
     */
    Object owner();
}
