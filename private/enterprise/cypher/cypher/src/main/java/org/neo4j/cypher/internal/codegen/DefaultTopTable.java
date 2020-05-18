/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.codegen;

import java.util.Comparator;

import org.neo4j.cypher.internal.collection.DefaultComparatorTopTable;

/**
 * The default implementation of a Top N table used by the generated code.
 */
public class DefaultTopTable<T extends Comparable<Object>> extends DefaultComparatorTopTable<T>
{
    public DefaultTopTable( int totalCount )
    {
        super( Comparator.naturalOrder(), totalCount );
    }
}
