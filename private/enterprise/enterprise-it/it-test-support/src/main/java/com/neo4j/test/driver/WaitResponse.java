/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import org.neo4j.driver.Record;

public class WaitResponse
{
    private static final String ADDRESS = "address";
    private static final String STATE = "state";
    private static final String MESSAGE = "message";
    private static final String SUCCESS = "success";
    private final String address;
    private final WaitResponseStates state;
    private final String message;
    private final boolean success;

    private WaitResponse( String address, WaitResponseStates state, String message, boolean success )
    {
        this.address = address;
        this.state = state;
        this.message = message;
        this.success = success;
    }

    public static WaitResponse createServerResponse( Record record )
    {
        if ( record.size() != 4 )
        {
            throw new IllegalArgumentException( "Expected exactly 4 values. Got " + record.size() );
        }
        var serverId = record.get( ADDRESS ).asString();
        var state = WaitResponseStates.valueOf( record.get( STATE ).asString() );
        var message = record.get( MESSAGE ).asString();
        var success = record.get( SUCCESS ).asBoolean();
        return new WaitResponse( serverId, state, message, success );
    }

    public WaitResponseStates state()
    {
        return state;
    }

    public String address()
    {
        return address;
    }

    public String message()
    {
        return message;
    }

    public boolean success()
    {
        return success;
    }

    @Override
    public String toString()
    {
        return "ServerResponse{" +
               "address='" + address + '\'' +
               ", state=" + state +
               ", message='" + message + '\'' +
               ", success='" + success + '\'' +
               '}';
    }
}
