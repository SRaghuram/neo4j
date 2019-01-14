/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;


import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

@ExtendWith( EphemeralFileSystemExtension.class )
public class EmbeddedConfiguredAuthScenariosInteractionIT extends ConfiguredAuthScenariosInteractionTestBase<EnterpriseLoginContext>
{
    @Inject
    EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Override
    protected NeoInteractionLevel<EnterpriseLoginContext> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new EmbeddedInteraction( config, () -> new UncloseableDelegatingFileSystemAbstraction( fileSystemRule.get() ) );
    }
}
