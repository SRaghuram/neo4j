/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.helper;

import java.net.ConnectException;
import java.util.function.Predicate;

public class IsConnectionException implements Predicate<Throwable>
{

    @Override
    public boolean test( Throwable e )
    {
        return e != null && (e instanceof ConnectException || test( e.getCause() ));
    }
}
