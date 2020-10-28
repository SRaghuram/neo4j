/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import java.util.Set;

import org.neo4j.driver.Result;

import static java.util.stream.Collectors.toSet;

public class WaitResponses
{
    public static WaitResponses create( Result result )
    {
        return new WaitResponses( result.stream()
                                        .map( WaitResponse::createServerResponse )
                                        .collect( toSet() ) );
    }

    private final Set<WaitResponse> waitResponse;

    private WaitResponses( Set<WaitResponse> waitResponse )
    {
        this.waitResponse = waitResponse;
    }

    public boolean successful()
    {
        return !waitResponse.isEmpty() && waitResponse.stream().allMatch( waitResponse -> waitResponse.state() == WaitResponseStates.CaughtUp );
    }

    public Set<WaitResponse> responses()
    {
        return waitResponse;
    }

    @Override
    public String toString()
    {
        return "ServerResponses{" +
               "serverResponses=" + waitResponse +
               "successful=" + successful() +
               '}';
    }
}
