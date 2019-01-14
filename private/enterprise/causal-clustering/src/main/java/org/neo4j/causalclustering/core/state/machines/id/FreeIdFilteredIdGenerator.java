/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import java.util.function.BooleanSupplier;

import org.neo4j.kernel.impl.store.id.IdGenerator;

/**
 * Id generator that will perform filtering of ids to free using supplied condition.
 * Id will be freed only if condition is true, otherwise it will be ignored.
 */
public class FreeIdFilteredIdGenerator extends IdGenerator.Delegate
{
    private final BooleanSupplier freeIdCondition;

    FreeIdFilteredIdGenerator( IdGenerator delegate, BooleanSupplier freeIdCondition )
    {
        super( delegate );
        this.freeIdCondition = freeIdCondition;
    }

    @Override
    public void freeId( long id )
    {
        if ( freeIdCondition.getAsBoolean() )
        {
            super.freeId( id );
        }
    }
}
