/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.FunctionSegment;
import org.neo4j.internal.kernel.api.security.LabelSegment;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.internal.kernel.api.security.RelTypeSegment;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.internal.kernel.api.security.UserSegment;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DBMS_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MATCH;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.READ;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.START_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.STOP_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRANSACTION_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRAVERSE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_35;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_36;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_40;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_41;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_41D1;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_42D4;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_42D6;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_42D7;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_43D1;

class EnterpriseSecurityComponentUpgradeIT extends SecurityGraphCompatibilityTestBase
{
    private static final String[] SUPPORTED_VERSIONS = {VERSION_35, VERSION_36, VERSION_40, VERSION_41D1, VERSION_41,
                                                        VERSION_42D4, VERSION_42D6, VERSION_42D7, VERSION_43D1};

    @ParameterizedTest
    @MethodSource( "supportedVersions" )
    void shouldUpgrade( String version ) throws Exception
    {
        // GIVEN
        initEnterprise( version );

        // WHEN
        enterpriseComponent.upgradeToCurrent( system );

        // THEN
        try ( Transaction tx = system.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            for ( String role : PredefinedRoles.roles )
            {
                assertPrivilegesForRole( tx, role, version );
            }
        }
    }

    private void assertPrivilegesForRole( Transaction tx, String role, String fromVersion ) throws InvalidArgumentsException
    {
        List<ResourcePrivilege> readPrivileges = new ArrayList<>();
        switch ( fromVersion )
        {
        case VERSION_35:
        case VERSION_36:
        case VERSION_40:
            ResourcePrivilege readNodePrivilege =
                    new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
            ResourcePrivilege readRelPrivilege =
                    new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
            ResourcePrivilege traverseNodePrivilege =
                    new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
            ResourcePrivilege traverseRelPrivilege =
                    new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL, ResourcePrivilege.SpecialDatabase.ALL );

            readPrivileges.add( readNodePrivilege );
            readPrivileges.add( readRelPrivilege );
            readPrivileges.add( traverseNodePrivilege );
            readPrivileges.add( traverseRelPrivilege );
            break;
        default:
            ResourcePrivilege matchNodePrivilege =
                    new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
            ResourcePrivilege matchRelPrivilege =
                    new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
            readPrivileges.add( matchNodePrivilege );
            readPrivileges.add( matchRelPrivilege );
        }
        ResourcePrivilege defaultAccessPrivilege =
                new ResourcePrivilege( GRANT, ACCESS, new Resource.DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.DEFAULT );
        ResourcePrivilege executeProcedurePrivilege =
                new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), ProcedureSegment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        ResourcePrivilege executeFunctionPrivilege =
                new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), FunctionSegment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        ResourcePrivilege accessPrivilege =
                new ResourcePrivilege( GRANT, ACCESS, new Resource.DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        ResourcePrivilege writeNodePrivilege =
                new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), LabelSegment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        ResourcePrivilege writeRelPrivilege =
                new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), RelTypeSegment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        ResourcePrivilege tokenNodePrivilege =
                new ResourcePrivilege( GRANT, TOKEN, new Resource.DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        ResourcePrivilege indexNodePrivilege =
                new ResourcePrivilege( GRANT, INDEX, new Resource.DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        ResourcePrivilege constraintNodePrivilege =
                new ResourcePrivilege( GRANT, CONSTRAINT, new Resource.DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        ResourcePrivilege startDbPrivilege =
                new ResourcePrivilege( GRANT, START_DATABASE, new Resource.DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        ResourcePrivilege stopDbPrivilege =
                new ResourcePrivilege( GRANT, STOP_DATABASE, new Resource.DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        ResourcePrivilege transactionPrivilege =
                new ResourcePrivilege( GRANT, TRANSACTION_MANAGEMENT, new Resource.DatabaseResource(), UserSegment.ALL, ResourcePrivilege.SpecialDatabase.ALL );
        ResourcePrivilege dbmsPrivilege =
                new ResourcePrivilege( GRANT, DBMS_ACTIONS, new Resource.DatabaseResource(), Segment.ALL, ResourcePrivilege.SpecialDatabase.ALL );

        Set<ResourcePrivilege> privileges;
        privileges = enterpriseComponent.getPrivilegesForRole( tx, role );

        List<ResourcePrivilege> expected = new ArrayList<>();
        switch ( role )
        {
        case PredefinedRoles.ADMIN:
            expected.add( accessPrivilege );
            expected.addAll( readPrivileges );
            expected.add( writeNodePrivilege );
            expected.add( writeRelPrivilege );
            expected.add( tokenNodePrivilege );
            expected.add( indexNodePrivilege );
            expected.add( constraintNodePrivilege );
            expected.add( startDbPrivilege );
            expected.add( stopDbPrivilege );
            expected.add( transactionPrivilege );
            expected.add( dbmsPrivilege );
            assertThat( privileges ).containsExactlyInAnyOrderElementsOf( expected );
            break;
        case PredefinedRoles.ARCHITECT:

            expected.add( accessPrivilege );
            expected.addAll( readPrivileges );
            expected.add( writeNodePrivilege );
            expected.add( writeRelPrivilege );
            expected.add( tokenNodePrivilege );
            expected.add( indexNodePrivilege );
            expected.add( constraintNodePrivilege );
            assertThat( privileges ).containsExactlyInAnyOrderElementsOf( expected );
            break;
        case PredefinedRoles.PUBLISHER:
            expected.add( accessPrivilege );
            expected.addAll( readPrivileges );
            expected.add( writeNodePrivilege );
            expected.add( writeRelPrivilege );
            expected.add( tokenNodePrivilege );
            assertThat( privileges ).containsExactlyInAnyOrderElementsOf( expected );
            break;
        case PredefinedRoles.EDITOR:
            expected.add( accessPrivilege );
            expected.addAll( readPrivileges );
            expected.add( writeNodePrivilege );
            expected.add( writeRelPrivilege );
            assertThat( privileges ).containsExactlyInAnyOrderElementsOf( expected );
            break;
        case PredefinedRoles.READER:
            expected.add( accessPrivilege );
            expected.addAll( readPrivileges );
            assertThat( privileges ).containsExactlyInAnyOrderElementsOf( expected );
            break;
        case PredefinedRoles.PUBLIC:
            if ( fromVersion.equals( VERSION_35 ) || fromVersion.equals( VERSION_36 ) || fromVersion.equals( VERSION_40 ) )
            {
                assertThat( privileges ).containsExactlyInAnyOrder(
                        executeProcedurePrivilege,
                        executeFunctionPrivilege
                );
            }
            else
            {
                assertThat( privileges ).containsExactlyInAnyOrder(
                        defaultAccessPrivilege,
                        executeProcedurePrivilege,
                        executeFunctionPrivilege
                );
            }
            break;
        default:
            fail( "unexpected role: " + role );
        }
    }

    private static Stream<Arguments> supportedVersions()
    {
        return Arrays.stream( SUPPORTED_VERSIONS ).map( Arguments::of );
    }
}
