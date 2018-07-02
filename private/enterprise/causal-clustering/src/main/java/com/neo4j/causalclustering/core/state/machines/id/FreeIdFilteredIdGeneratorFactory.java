/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import java.io.File;
import java.nio.file.OpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;

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
    public IdGenerator open( File filename, IdType idType, LongSupplier highIdScanner, long maxId, OpenOption... openOptions )
    {
        FreeIdFilteredIdGenerator freeIdFilteredIdGenerator =
                new FreeIdFilteredIdGenerator( delegate.open( filename, idType, highIdScanner, maxId ), freeIdCondition );
        delegatedGenerator.put( idType, freeIdFilteredIdGenerator );
        return freeIdFilteredIdGenerator;
    }

    @Override
    public IdGenerator create( File filename, IdType idType, long highId, boolean throwIfFileExists, long maxId, OpenOption... openOptions )
    {
        return delegate.create( filename, idType, highId, throwIfFileExists, maxId, openOptions );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return delegatedGenerator.get( idType );
    }
}
