/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import com.neo4j.causalclustering.error_handling.Panicker;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.configuration.IdTypeConfiguration;
import org.neo4j.internal.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.LogProvider;

public class ReplicatedIdGeneratorFactory implements IdGeneratorFactory
{
    private final Map<Pair<DatabaseId,IdType>,ReplicatedIdGenerator> generators = new HashMap<>();
    private final FileSystemAbstraction fs;
    private final Function<DatabaseId,ReplicatedIdRangeAcquirer> idRangeAcquirer;
    private final LogProvider logProvider;
    private final Panicker panicker;
    private IdTypeConfigurationProvider idTypeConfigurationProvider;
    private final DatabaseId databaseId;

    public ReplicatedIdGeneratorFactory( FileSystemAbstraction fs, Function<DatabaseId,ReplicatedIdRangeAcquirer> idRangeAcquirer,
            LogProvider logProvider, IdTypeConfigurationProvider idTypeConfigurationProvider, DatabaseId databaseId, Panicker panicker )
    {
        this.fs = fs;
        this.idRangeAcquirer = idRangeAcquirer;
        this.logProvider = logProvider;
        this.idTypeConfigurationProvider = idTypeConfigurationProvider;
        this.databaseId = databaseId;
        this.panicker = panicker;
    }

    @Override
    public IdGenerator open( File file, IdType idType, LongSupplier highId, long maxId )
    {
        IdTypeConfiguration idTypeConfiguration = idTypeConfigurationProvider.getIdTypeConfiguration( idType );
        return openGenerator( file, idTypeConfiguration.getGrabSize(), idType, highId, maxId, idTypeConfiguration.allowAggressiveReuse() );
    }

    @Override
    public IdGenerator open( File fileName, int grabSize, IdType idType, LongSupplier highId, long maxId )
    {
        IdTypeConfiguration idTypeConfiguration = idTypeConfigurationProvider.getIdTypeConfiguration( idType );
        return openGenerator( fileName, grabSize, idType, highId, maxId, idTypeConfiguration.allowAggressiveReuse() );
    }

    private IdGenerator openGenerator( File file, int grabSize, IdType idType, LongSupplier highId, long maxId, boolean aggressiveReuse )
    {
        ReplicatedIdGenerator other = generators.get( Pair.of( databaseId, idType ) );
        if ( other != null )
        {
            other.close();
        }
        ReplicatedIdGenerator replicatedIdGenerator = new ReplicatedIdGenerator( fs, file, idType, highId,
                idRangeAcquirer.apply( databaseId ), logProvider, grabSize, aggressiveReuse, panicker );

        generators.put( Pair.of( databaseId, idType ), replicatedIdGenerator);
        return replicatedIdGenerator;
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return get( databaseId, idType );
    }

    public IdGenerator get( DatabaseId databaseId, IdType idType )
    {
        return generators.get( Pair.of( databaseId, idType ) );
    }

    @Override
    public void create( File fileName, long highId, boolean throwIfFileExists )
    {
        ReplicatedIdGenerator.createGenerator( fs, fileName, highId, throwIfFileExists );
    }
}
