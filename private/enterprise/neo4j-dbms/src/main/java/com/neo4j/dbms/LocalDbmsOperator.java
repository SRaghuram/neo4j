/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;

/**
 * Database operator for local administrative overrides of system-wide operator.
 *
 */
public final class LocalDbmsOperator extends DbmsOperator
{
    public void dropDatabase( NamedDatabaseId id )
    {
        desired.put( id.name(), new EnterpriseDatabaseState( id, DROPPED ) );
        trigger( ReconcilerRequest.priorityTarget( id ).build() ).await( id );
    }

    public void startDatabase( NamedDatabaseId id )
    {
        desired.put( id.name(), new EnterpriseDatabaseState( id, STARTED ) );
        trigger( ReconcilerRequest.priorityTarget( id ).build() ).await( id );
    }

    public void stopDatabase( NamedDatabaseId id )
    {
        desired.put( id.name(), new EnterpriseDatabaseState( id, STOPPED ) );
        trigger( ReconcilerRequest.priorityTarget( id ).build() ).await( id );
    }
}
