/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.codegen;

import java.util.ArrayList;

/**
 * The default implementation of a table used for a full sort by the generated code
 *
 * It accepts tuples as boxed objects that implements Comparable
 *
 * Implements the following interface:
 * (since the code is generated it does not actually need to declare it with implements)
 *
 * public interface SortTable<T extends Comparable<?>>
 * {
 *     boolean add( T e );
 *
 *     void sort();
 *
 *     Iterator<T> iterator();
 * }
 *
 * This implementation just adapts Java's standard ArrayList
 */
public class DefaultFullSortTable<T extends Comparable<?>> extends ArrayList<T> // implements SortTable<T>
{
    public DefaultFullSortTable( int initialCapacity )
    {
        super( initialCapacity );
    }

    public void sort()
    {
        // Sort using the default array sort implementation (currently ComparableTimSort)
        sort( null );
    }
}
