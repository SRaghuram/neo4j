/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.util;

public class HeapDumpTestClasses
{
    static class Empty {}
    static class MultiDimObjectArray
    {
        public Empty[][] empties = new Empty[][]{{new Empty()}, {new Empty()}};
    }

    static class MultiDimLongArray
    {
        public long[][] longs = new long[][]{{123L}, {567L, 1000100100L}};
    }

    static class MultiDimByteArray
    {
        public byte[][][] bytes = new byte[][][]{{{0}, {1, 2}}, {{3}}};
    }

}
