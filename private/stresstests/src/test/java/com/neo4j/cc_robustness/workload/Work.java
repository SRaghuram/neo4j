/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload;

import java.io.Serializable;

import org.neo4j.graphdb.GraphDatabaseService;

public interface Work extends Serializable
{
    void doWork( GraphDatabaseService db );
}
