/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;
import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.unmodifiableMap;

public class TestDbmsOperator implements DbmsOperator
{
    private final Map<DatabaseId,OperatorState> desired;
    private OperatorConnector connector;

    TestDbmsOperator( Map<DatabaseId,OperatorState> desired )
    {
        this.desired = desired;
    }

    @Override
    public void connect( OperatorConnector connector )
    {
        Objects.requireNonNull( connector );
        this.connector = connector;
        connector.register( this );
        connector.trigger();
    }

    @Override
    public Map<DatabaseId,OperatorState> getDesired()
    {
        return unmodifiableMap( desired );
    }
}
