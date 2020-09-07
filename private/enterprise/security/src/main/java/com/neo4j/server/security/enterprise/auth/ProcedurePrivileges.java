/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.eclipse.collections.api.set.primitive.IntSet;

class ProcedurePrivileges
{
    private final boolean allowExecuteAllProcedures;
    private final boolean disallowExecuteAllProcedures;
    private final IntSet allowExecuteProcedures;
    private final IntSet disallowExecuteProcedures;

    ProcedurePrivileges( boolean allowExecuteAllProcedures,
                         boolean disallowExecuteAllProcedures,
                         IntSet allowExecuteProcedures,
                         IntSet disallowExecuteProcedures )
    {

        this.allowExecuteAllProcedures = allowExecuteAllProcedures;
        this.disallowExecuteAllProcedures = disallowExecuteAllProcedures;
        this.allowExecuteProcedures = allowExecuteProcedures;
        this.disallowExecuteProcedures = disallowExecuteProcedures;
    }

    boolean allowsExecuteProcedure( int procedureId )
    {
        if ( disallowExecuteAllProcedures || disallowExecuteProcedures.contains( procedureId ) )
        {
            return false;
        }
        return allowExecuteAllProcedures || allowExecuteProcedures.contains( procedureId );
    }
}