/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.id;

import java.io.File;
import java.nio.file.OpenOption;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Wraps {@link IdGenerator} so that ids can be {@link IdGenerator#freeId(long) freed} at safe points in time, after all transactions
 * which were active at the time of freeing, have been closed.
 */
public class BufferingIdGeneratorFactory implements IdGeneratorFactory
{
    private final BufferingIdGenerator[/*IdType#ordinal as key*/] overriddenIdGenerators =
            new BufferingIdGenerator[IdType.values().length];
    private Supplier<IdController.ConditionSnapshot> boundaries;
    private final Predicate<IdController.ConditionSnapshot> safeThreshold;
    private final IdGeneratorFactory delegate;

    public BufferingIdGeneratorFactory( IdGeneratorFactory delegate )
    {
        this.delegate = delegate;
        this.safeThreshold = IdController.ConditionSnapshot::conditionMet;
    }

    public void initialize( Supplier<IdController.ConditionSnapshot> conditionSnapshotSupplier )
    {
        boundaries = conditionSnapshotSupplier;
    }

    @Override
    public IdGenerator open( File filename, IdType idType, LongSupplier highIdScanner, long maxId, OpenOption... openOptions )
    {
        assert boundaries != null : "Factory needs to be initialized before usage";

        IdGenerator generator = delegate.open( filename, idType, highIdScanner, maxId );
        return wrapAndKeep( idType, generator );
    }

    @Override
    public IdGenerator create( File filename, IdType idType, long highId, boolean throwIfFileExists, long maxId, OpenOption... openOptions )
    {
        IdGenerator idGenerator = delegate.create( filename, idType, highId, throwIfFileExists, maxId, openOptions );
        return wrapAndKeep( idType, idGenerator );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        IdGenerator generator = overriddenIdGenerators[idType.ordinal()];
        return generator != null ? generator : delegate.get( idType );
    }

    private IdGenerator wrapAndKeep( IdType idType, IdGenerator generator )
    {
        BufferingIdGenerator bufferingGenerator = new BufferingIdGenerator( generator );
        bufferingGenerator.initialize( boundaries, safeThreshold );
        overriddenIdGenerators[idType.ordinal()] = bufferingGenerator;
        return bufferingGenerator;
    }

    public void maintenance()
    {
        for ( BufferingIdGenerator generator : overriddenIdGenerators )
        {
            if ( generator != null )
            {
                generator.maintenance();
            }
        }
    }

    public void clear()
    {
        for ( BufferingIdGenerator generator : overriddenIdGenerators )
        {
            if ( generator != null )
            {
                generator.clear();
            }
        }
    }
}
