/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import java.util.function.BooleanSupplier;

import org.neo4j.kernel.impl.store.id.IdGenerator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class FreeIdFilteredIdGeneratorTest
{

    private IdGenerator idGenerator = mock( IdGenerator.class );

    @Test
    public void freeIdIfConditionSatisfied()
    {
        FreeIdFilteredIdGenerator generator = createFilteredIdGenerator( idGenerator, () -> true );
        generator.freeId( 1 );

        verify( idGenerator ).freeId( 1 );
    }

    @Test
    public void skipFreeIdIfConditionIsNotSatisfied()
    {
        FreeIdFilteredIdGenerator generator = createFilteredIdGenerator( idGenerator, () -> false );
        generator.freeId( 1 );

        verifyZeroInteractions( idGenerator );
    }

    @Test
    public void freeIdOnlyWhenConditionSatisfied()
    {
        MutableBoolean condition = new MutableBoolean();
        FreeIdFilteredIdGenerator generator = createFilteredIdGenerator( idGenerator, condition::booleanValue );
        generator.freeId( 1 );
        condition.setTrue();
        generator.freeId( 2 );

        verify( idGenerator ).freeId( 2 );
    }

    private FreeIdFilteredIdGenerator createFilteredIdGenerator( IdGenerator idGenerator,
            BooleanSupplier booleanSupplier )
    {
        return new FreeIdFilteredIdGenerator( idGenerator, booleanSupplier );
    }
}
