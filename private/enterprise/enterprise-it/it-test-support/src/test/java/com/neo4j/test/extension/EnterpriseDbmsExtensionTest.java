/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension;

import com.neo4j.test.extension.dbms.EnterpriseDbmsExtensionEnforceAnnotations;
import com.neo4j.test.extension.dbms.EnterpriseDbmsExtensionMixImpermanent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContextException;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;

import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

class EnterpriseDbmsExtensionTest
{
    @Test
    void enforceAnnotation()
    {
        Events testEvents = EngineTestKit.engine( ENGINE_ID )
                                         .selectors( selectClass( EnterpriseDbmsExtensionEnforceAnnotations.class ) ).execute()
                                         .allEvents();

        testEvents.assertThatEvents().haveExactly( 1,
                event( finishedWithFailure( instanceOf( IllegalArgumentException.class ),
                        message( message -> message.contains( "must be annotated" ) ) ) ) );
    }

    @Test
    void mixImpermanent()
    {
        Events testEvents = EngineTestKit.engine( ENGINE_ID )
                                         .selectors( selectClass( EnterpriseDbmsExtensionMixImpermanent.NestedTest.class ) ).execute()
                                         .allEvents();

        testEvents.assertThatEvents().haveExactly( 1,
                event( finishedWithFailure( instanceOf( ExtensionContextException.class ) ) ) );
    }
}
