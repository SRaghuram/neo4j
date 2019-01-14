/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.id;

import org.neo4j.kernel.impl.store.id.IdRange;

public final class IdAllocation
{
    private final IdRange idRange;
    private final long highestIdInUse;
    private final long defragCount;

    public IdAllocation( IdRange idRange, long highestIdInUse, long defragCount )
    {
        this.idRange = idRange;
        this.highestIdInUse = highestIdInUse;
        this.defragCount = defragCount;
    }

    public long getHighestIdInUse()
    {
        return highestIdInUse;
    }

    public long getDefragCount()
    {
        return defragCount;
    }

    public IdRange getIdRange()
    {
        return this.idRange;
    }

    @Override
    public String toString()
    {
        return "IdAllocation[" + idRange + ", " + highestIdInUse + ", " + defragCount + "]";
    }
}
