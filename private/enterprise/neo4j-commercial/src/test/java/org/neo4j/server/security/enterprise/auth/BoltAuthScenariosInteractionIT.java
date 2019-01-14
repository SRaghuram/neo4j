/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;


import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

@ExtendWith( EphemeralFileSystemExtension.class )
class BoltAuthScenariosInteractionIT extends AuthScenariosInteractionTestBase<BoltInteraction.BoltSubject>
{
    @Inject
    EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    BoltAuthScenariosInteractionIT()
    {
        super();
        IS_EMBEDDED = false;
    }

    @Override
    public NeoInteractionLevel<BoltInteraction.BoltSubject> setUpNeoServer( Map<String,String> config )
    {
        return new BoltInteraction( config,
                () -> new UncloseableDelegatingFileSystemAbstraction( fileSystemRule.get() ) );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        return ValueUtils.of( obj );
    }
}
