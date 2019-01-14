/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.master;

public interface SlavePriority
{
    Iterable<Slave> prioritize( Iterable<Slave> slaves );
}
