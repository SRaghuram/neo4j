/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test;

import com.neo4j.enterprise.edition.EnterpriseEditionModule;
import com.neo4j.security.NullSecurityLog;
import com.neo4j.server.security.enterprise.log.SecurityLog;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;

class TestEnterpriseEditionModule extends EnterpriseEditionModule
{
    private final boolean impermanent;

    TestEnterpriseEditionModule( GlobalModule globalModule, boolean impermanent )
    {
        super( globalModule );
        this.impermanent = impermanent;
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule )
    {
        super.createSecurityModule( globalModule );
    }

    @Override
    public SecurityLog makeSecurityLog( Config config )
    {
        return impermanent ? new NullSecurityLog() : super.makeSecurityLog( config );
    }
}
