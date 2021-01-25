/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

public enum TransactionOutcome
{
    // All good
    success( "S" ),

    // Deadlock detected
    deadlock( "D" ),

    // Transient failure
    transient_failure( "T" ),

    // Failure, although boring and not worthy printing
    boring_failure( "_" ),

    // Failure, although considered benign and interesting to print
    benign_failure( "B" ),

    // Failure, probably quite interesting to have a look at, so stack trace should be printed
    unknown_failure( "F" );

    private final String shortName;

    TransactionOutcome( String shortName )
    {
        this.shortName = shortName;
    }

    @Override
    public String toString()
    {
        return shortName;
    }
}
