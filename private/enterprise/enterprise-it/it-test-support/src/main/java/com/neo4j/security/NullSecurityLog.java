/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.server.security.enterprise.log.SecurityLog;

import org.neo4j.logging.NullLogProvider;

public class NullSecurityLog extends SecurityLog
{
    public NullSecurityLog()
    {
        super( NullLogProvider.getInstance().getLog( "" ) );
    }

    @Override
    public void init()
    {
        //do nothing
    }

    @Override
    public void shutdown()
    {
        //do nothing
    }
}
