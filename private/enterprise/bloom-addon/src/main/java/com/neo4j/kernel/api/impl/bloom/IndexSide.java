/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

public enum IndexSide
{
    SIDE_A, SIDE_B;

    public IndexSide otherSide()
    {
        return this == SIDE_A ? SIDE_B : SIDE_A;
    }
}
