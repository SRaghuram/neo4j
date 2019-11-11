/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.utils;

import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

public final class DriverUtils
{
    private DriverUtils()
    {

    }

    public static <T> T inMegaTx( Driver driver, Function<Transaction, T> workload )
    {
        try ( var session = driver.session( SessionConfig.builder().withDatabase( "mega" ).build() ) )
        {
            return session.writeTransaction( workload::apply );
        }
    }

    public static <T> T inMegaTx( Driver driver, AccessMode accessMode, Function<Transaction, T> workload )
    {
        try ( var session = driver.session( SessionConfig.builder().withDatabase( "mega" ).withDefaultAccessMode( accessMode ).build() ) )
        {
            return session.writeTransaction( workload::apply );
        }
    }

    public static void doInMegaTx( Driver driver, Consumer<Transaction> workload )
    {
        inMegaTx( driver, tx ->
        {
            workload.accept( tx );
            return null;
        } );
    }

    public static void doInMegaTx( Driver driver, AccessMode accessMode, Consumer<Transaction> workload )
    {
        inMegaTx( driver, accessMode, tx ->
        {
            workload.accept( tx );
            return null;
        } );
    }

    public static <T> T inMegaSession( Driver driver, Function<Session, T> workload )
    {
        try ( var session = driver.session( SessionConfig.builder().withDatabase( "mega" ).build() ) )
        {
            return workload.apply( session );
        }
    }

    public static void doInMegaSession( Driver driver, Consumer<Session> workload )
    {
        try ( var session = driver.session( SessionConfig.builder().withDatabase( "mega" ).build() ) )
        {
            workload.accept( session );
        }
    }
}
