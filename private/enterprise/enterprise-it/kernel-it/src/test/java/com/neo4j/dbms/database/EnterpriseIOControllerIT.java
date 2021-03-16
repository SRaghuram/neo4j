/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.kernel.impl.pagecache.iocontroller.ConfigurableIOController;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.IOController;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@EnterpriseDbmsExtension
class EnterpriseIOControllerIT
{
    @Inject
    private IOController ioController;

    @Test
    void useEnterpriseIOController()
    {
        assertThat( ioController ).isInstanceOf( ConfigurableIOController.class );
    }
}
