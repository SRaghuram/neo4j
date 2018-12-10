/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;

public class FreeIdFilteredIdGeneratorFactory implements IdGeneratorFactory
{
    private Map<IdType, IdGenerator> delegatedGenerator = new HashMap<>();
    private final IdGeneratorFactory delegate;
    private final BooleanSupplier freeIdCondition;

    public FreeIdFilteredIdGeneratorFactory( IdGeneratorFactory delegate, BooleanSupplier freeIdCondition )
    {
        this.delegate = delegate;
        this.freeIdCondition = freeIdCondition;
    }

    @Override
    public IdGenerator open( File filename, IdType idType, LongSupplier highId, long maxId )
    {
        FreeIdFilteredIdGenerator freeIdFilteredIdGenerator =
                new FreeIdFilteredIdGenerator( delegate.open( filename, idType, highId, maxId ), freeIdCondition );
        delegatedGenerator.put( idType, freeIdFilteredIdGenerator );
        return freeIdFilteredIdGenerator;
    }

    @Override
    public IdGenerator open( File filename, int grabSize, IdType idType, LongSupplier highId, long maxId )
    {
        FreeIdFilteredIdGenerator freeIdFilteredIdGenerator =
                new FreeIdFilteredIdGenerator( delegate.open( filename, grabSize, idType, highId, maxId ),
                        freeIdCondition );
        delegatedGenerator.put( idType, freeIdFilteredIdGenerator );
        return freeIdFilteredIdGenerator;
    }

    @Override
    public void create( File filename, long highId, boolean throwIfFileExists )
    {
        delegate.create( filename, highId, throwIfFileExists );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return delegatedGenerator.get( idType );
    }
}
