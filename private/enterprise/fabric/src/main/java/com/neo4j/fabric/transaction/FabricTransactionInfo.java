/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.transaction;

import java.time.Duration;
import java.util.Map;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;

public class FabricTransactionInfo
{
    private final AccessMode accessMode;
    private final LoginContext loginContext;
    private final ClientConnectionInfo clientConnectionInfo;
    private final String databaseName;
    private final boolean implicitTransaction;
    private final Duration txTimeout;
    private final Map<String,Object> txMetadata;

    public FabricTransactionInfo( AccessMode accessMode,
            LoginContext loginContext,
            ClientConnectionInfo clientConnectionInfo,
            String databaseName,
            boolean implicitTransaction,
            Duration txTimeout,
            Map<String,Object> txMetadata )
    {
        this.accessMode = accessMode;
        this.loginContext = loginContext;
        this.clientConnectionInfo = clientConnectionInfo;
        this.databaseName = databaseName;
        this.implicitTransaction = implicitTransaction;
        this.txTimeout = txTimeout;
        this.txMetadata = txMetadata;
    }

    public AccessMode getAccessMode()
    {
        return accessMode;
    }

    public LoginContext getLoginContext()
    {
        return loginContext;
    }

    public ClientConnectionInfo getClientConnectionInfo()
    {
        return clientConnectionInfo;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public boolean isImplicitTransaction()
    {
        return implicitTransaction;
    }

    public Duration getTxTimeout()
    {
        return txTimeout;
    }

    public Map<String,Object> getTxMetadata()
    {
        return txMetadata;
    }
}
