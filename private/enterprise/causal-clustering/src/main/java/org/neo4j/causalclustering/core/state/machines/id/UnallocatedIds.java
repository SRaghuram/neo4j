/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import org.neo4j.kernel.impl.store.id.IdType;

interface UnallocatedIds
{
    /**
     * @param idType the type of graph object whose ID is under allocation
     * @return the first unallocated entry for idType
     */
    long firstUnallocated( IdType idType );
}
