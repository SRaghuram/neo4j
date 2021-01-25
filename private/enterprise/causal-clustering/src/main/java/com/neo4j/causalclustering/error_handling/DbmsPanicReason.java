/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import java.util.Objects;

/**
 * Panic the Database Management System (DBMS). A panic indicates that we have encountered an error that we do not expect the Database Management System to be
 * able to recover from. How the neo4j process reacts to a DBMS panic depends on the configuration set by the user.
 * <p>
 * Important: For the purposes of panics the DBMS and individual databases are considered to be loosely coupled. This means that a DBMS panic does not cause a
 * panic of any databases. And vice-versa a database panic does not cause a DBMS panic, with the single exception that a system db panic will cause a DBMS panic
 * because the system db is an essential part of the DBMS.
 */
public enum DbmsPanicReason implements Panicker.Reason
{
    Test( "DBMSPanic just for testing", 0 ),
    UnexpectedReason( "See exception message for details", 1 ),
    SystemDbPanicked( "The system database panicked", 2 ),
    IrrecoverableDiscoveryFailure( "Discovery system failed irrecoverably", 3 );

    private final String description;
    final int exitCode;

    DbmsPanicReason( String description, int exitCode )
    {
        this.description = description;
        this.exitCode = exitCode;
    }

    public String getDescription()
    {
        return description;
    }

    @Override
    public String toString()
    {
        return "DBMSPanic{" +
               "description='" + description + '\'' +
               ", exitCode=" + exitCode +
               '}';
    }
}
