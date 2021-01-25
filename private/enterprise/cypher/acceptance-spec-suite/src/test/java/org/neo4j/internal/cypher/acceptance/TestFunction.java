/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class TestFunction
{
    @Context
    public Transaction transaction;

    @UserFunction( "test.toSet" )
    public List<Object> toSet( @Name( "values" ) List<Object> list )
    {
        return new ArrayList<>( new LinkedHashSet<>( list ) );
    }

    @UserFunction( "test.nodeList" )
    public List<Object> nodeList()
    {
        Result result = transaction.execute( "MATCH (n) RETURN n LIMIT 1" );
        Object node = result.next().get( "n" );
        result.close();
        return Collections.singletonList( node );
    }

    @UserFunction( "test.sum" )
    public double sum( @Name( "numbers" ) List<Number> list )
    {
        double sum = 0;
        for ( Number number : list )
        {
            sum += number.doubleValue();
        }
        return sum;
    }
}
