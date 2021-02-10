/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common;

import com.neo4j.bench.model.model.BranchAndVersion;
import com.neo4j.bench.model.model.Repository;

public class ParameterVerifier
{
    private static final String DEFAULT_TRIGGER = "neo4j";

    public static void performSanityChecks( String neo4jBranchOwner, String neo4jVersion, String neo4jBranch, String triggeredBy )
    {
        if ( !BranchAndVersion.isPersonalBranch( Repository.NEO4J, neo4jBranchOwner ) && triggeredBy.equals( DEFAULT_TRIGGER ) )
        {
            BranchAndVersion.assertBranchEqualsSeries( neo4jVersion, neo4jBranch );
        }
    }
}
