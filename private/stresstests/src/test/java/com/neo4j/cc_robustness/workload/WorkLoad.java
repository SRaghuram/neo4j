/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload;

import com.neo4j.cc_robustness.Orchestrator;

import org.neo4j.logging.LogProvider;
import org.neo4j.test.DbRepresentation;

public interface WorkLoad
{
    void perform( Orchestrator orchestrator, LogProvider logProvider ) throws Exception;

    void forceShutdown();
}
