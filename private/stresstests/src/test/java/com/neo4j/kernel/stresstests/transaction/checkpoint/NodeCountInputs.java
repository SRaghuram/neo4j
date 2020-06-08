/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.stresstests.transaction.checkpoint;

import java.io.IOException;

import org.neo4j.internal.batchimport.GeneratingInputIterator;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.PropertySizeCalculator;
import org.neo4j.internal.batchimport.input.ReadableGroups;

import static org.neo4j.internal.batchimport.input.Input.knownEstimates;

public class NodeCountInputs implements Input
{
    private static final Object[] properties = new Object[]{
            "a", 10, "b", 10, "c", 10, "d", 10, "e", 10, "f", 10, "g", 10, "h", 10, "i", 10,
            "j", 10, "k", 10, "l", 10, "m", 10, "o", 10, "p", 10, "q", 10, "r", 10, "s", 10
    };
    private static final String[] labels = new String[]{"a", "b", "c", "d"};

    private final long nodeCount;

    public NodeCountInputs( long nodeCount )
    {
        this.nodeCount = nodeCount;
    }

    @Override
    public InputIterable nodes( Collector badCollector )
    {
        return () -> new GeneratingInputIterator<>( nodeCount, 1_000, batch -> null,
                (GeneratingInputIterator.Generator<Void>) ( state, visitor, id ) -> {
                    visitor.id( id, Group.GLOBAL );
                    visitor.labels( labels );
                    for ( int i = 0; i < properties.length; i++ )
                    {
                        visitor.property( (String) properties[i++], properties[i] );
                    }
                }, 0 );
    }

    @Override
    public InputIterable relationships( Collector badCollector )
    {
        return GeneratingInputIterator.EMPTY_ITERABLE;
    }

    @Override
    public IdType idType()
    {
        return IdType.ACTUAL;
    }

    @Override
    public ReadableGroups groups()
    {
        return ReadableGroups.EMPTY;
    }

    @Override
    public Estimates calculateEstimates( PropertySizeCalculator valueSizeCalculator ) throws IOException
    {
        return knownEstimates( nodeCount, 0, nodeCount * properties.length / 2, 0, nodeCount * properties.length / 2 * Long.BYTES, 0,
                nodeCount * labels.length );
    }
}
