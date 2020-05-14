/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import org.apache.commons.lang3.SystemUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Environment
{
    private final String operatingSystem;
    private final String server;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public Environment()
    {
        this( "-1", "-1" );
    }

    public Environment( String operatingSystem, String server )
    {
        this.operatingSystem = requireNonNull( operatingSystem );
        this.server = requireNonNull( server );
    }

    public static Environment current()
    {
        return new Environment( currentOperatingSystem(), currentServer() );
    }

    public String operatingSystem()
    {
        return operatingSystem;
    }

    public String server()
    {
        return server;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        Environment that = (Environment) o;
        return Objects.equals( operatingSystem, that.operatingSystem ) &&
               Objects.equals( server, that.server );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( operatingSystem, server );
    }

    @Override
    public String toString()
    {
        return "Environment{" +
               "operatingSystem='" + operatingSystem + '\'' +
               ", server='" + server + '\'' +
               '}';
    }

    private static String currentOperatingSystem()
    {
        return SystemUtils.OS_NAME + ", " + SystemUtils.OS_VERSION;
    }

    private static String currentServer()
    {
        try
        {
            return InetAddress.getLocalHost().getHostName();
        }
        catch ( UnknownHostException e )
        {
            throw new RuntimeException( e );
        }
    }
}
