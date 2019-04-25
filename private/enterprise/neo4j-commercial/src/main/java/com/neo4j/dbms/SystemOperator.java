/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;

import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.emptyMap;

/**
 * Database operator for a system-wide operator.
 */
// TODO: Implement based on system database.
public class SystemOperator implements Operator
{
    private final OperatorConnector connector;

    public SystemOperator( OperatorConnector connector )
    {
        this.connector = connector;
    }

    @Override
    public Map<DatabaseId,OperatorState> getDesired()
    {
        return emptyMap();
    }
}
