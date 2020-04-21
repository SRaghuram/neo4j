/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseVersion_0_35;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseVersion_1_36;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseVersion_2_40;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseVersion_3_41d1;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseVersion_4_41d2;
import com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion;
import com.neo4j.server.security.enterprise.systemgraph.versions.NoEnterpriseComponentVersion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.AbstractSystemGraphComponent;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.KnownSystemComponentVersions;

import static com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion.ROLE_LABEL;
import static com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion.USER_LABEL;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

public class EnterpriseSecurityGraphComponent extends AbstractSystemGraphComponent
{
    private final Log log;
    private final UserRepository defaultAdminRepository;
    private final KnownSystemComponentVersions<KnownEnterpriseSecurityComponentVersion> knownSecurityComponentVersions =
            new KnownSystemComponentVersions<>( new NoEnterpriseComponentVersion() );
    public static final String COMPONENT = "security-privileges";

    public EnterpriseSecurityGraphComponent( Log log, RoleRepository migrationRoleRepository, UserRepository defaultAdminRepository, Config config )
    {
        super( config );
        this.log = log;
        this.defaultAdminRepository = defaultAdminRepository;
        knownSecurityComponentVersions.add( new EnterpriseVersion_0_35( log, migrationRoleRepository ) );
        knownSecurityComponentVersions.add( new EnterpriseVersion_1_36( log, config ) );
        knownSecurityComponentVersions.add( new EnterpriseVersion_2_40( log ) );
        knownSecurityComponentVersions.add( new EnterpriseVersion_3_41d1( log ) );
        knownSecurityComponentVersions.add( new EnterpriseVersion_4_41d2( log ) );
    }

    @Override
    public String component()
    {
        return COMPONENT;
    }

    @Override
    public Status detect( Transaction tx )
    {
        return knownSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx ).getStatus();
    }

    @Override
    public void initializeSystemGraphModel( Transaction tx ) throws Exception
    {
        initializeLatestSystemGraph( tx );
    }

    @Override
    public void initializeSystemGraphConstraints( Transaction tx )
    {
        initializeSystemGraphConstraint( tx, ROLE_LABEL, "name" );
    }

    @Override
    public Optional<Exception> upgradeToCurrent( Transaction tx )
    {
        KnownEnterpriseSecurityComponentVersion component = knownSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
        if ( component.version == NoEnterpriseComponentVersion.VERSION )
        {
            try
            {
                initializeLatestSystemGraph( tx );
            }
            catch ( Exception e )
            {
                return Optional.of( e );
            }
        }
        else
        {
            if ( component.migrationSupported() )
            {
                try
                {
                    component.upgradeSecurityGraph( tx, knownSecurityComponentVersions.latestSecurityGraphVersion() );
                }
                catch ( Exception e )
                {
                    return Optional.of( e );
                }
            }
            else
            {
                return Optional.of( component.unsupported() );
            }
        }
        return Optional.empty();
    }

    KnownEnterpriseSecurityComponentVersion findSecurityGraphComponentVersion( String substring )
    {
        return knownSecurityComponentVersions.findSecurityGraphVersion( substring );
    }

    Set<ResourcePrivilege> getPrivilegeForRoles( Transaction tx, List<String> roles, Cache<String,Set<ResourcePrivilege>> privilegeCache )
    {
        KnownEnterpriseSecurityComponentVersion version = knownSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
        if ( !version.runtimeSupported() )
        {
            return Collections.emptySet();
        }
        return version.getPrivilegeForRoles( tx, roles, privilegeCache );
    }

    private void initializeLatestSystemGraph( Transaction tx ) throws Exception
    {
        KnownEnterpriseSecurityComponentVersion latest = knownSecurityComponentVersions.latestSecurityGraphVersion();
        Map<String,Set<String>> admins = new HashMap<>();
        admins.put( PredefinedRoles.ADMIN, Set.of( decideOnAdminUsername( tx ) ) );
        latest.initializePrivileges( tx, PredefinedRoles.roles, admins );
        if ( config.isExplicitlySet( GraphDatabaseSettings.system_init_file ) )
        {
            doCustomSecurityInitialization( tx );
        }
    }

    /**
     * Tries to find an admin candidate among the existing users. Also supports the admin.ini file created by the neo4j-admin default-admin command which
     * supports upgrading from community to enterprise by selecting the future admin from all community users.
     */
    private String decideOnAdminUsername( Transaction tx ) throws Exception
    {
        String newAdmin = null;
        KnownEnterpriseSecurityComponentVersion latestComponent = knownSecurityComponentVersions.latestSecurityGraphVersion();
        Set<String> usernames = latestComponent.getAllNames( tx, USER_LABEL );

        // Try to determine who should be admin, by first checking the outcome of the SetDefaultAdmin command
        defaultAdminRepository.start();
        final int numberOfDefaultAdmins = defaultAdminRepository.numberOfUsers();
        if ( numberOfDefaultAdmins > 1 )
        {
            throw latestComponent.logAndCreateException( "No roles defined, and multiple users defined as default admin user. " + "Please use " +
                    "`neo4j-admin set-default-admin` to select a valid admin." );
        }
        else if ( numberOfDefaultAdmins == 1 )
        {
            newAdmin = defaultAdminRepository.getAllUsernames().iterator().next();
        }

        if ( newAdmin != null )
        {
            // We currently support only one default admin
            if ( !usernames.contains( newAdmin ) )
            {
                throw latestComponent.logAndCreateException( "No roles defined, and default admin user '" + newAdmin + "' does not exist. " +
                        "Please use `neo4j-admin set-default-admin` to select a valid admin." );
            }
            return newAdmin;
        }
        else if ( usernames.size() == 1 )
        {
            // If only a single user exists, make her an admin
            return usernames.iterator().next();
        }
        else if ( usernames.contains( INITIAL_USER_NAME ) )
        {
            // If the default neo4j user exists, make her an admin
            return INITIAL_USER_NAME;
        }
        else
        {
            throw latestComponent.logAndCreateException(
                    "No roles defined, and cannot determine which user should be admin. " + "Please use `neo4j-admin set-default-admin` to select an admin. " );
        }
    }

    private void doCustomSecurityInitialization( Transaction tx ) throws IOException
    {
        // this is first startup and custom initialization specified
        File initFile = config.get( GraphDatabaseSettings.system_init_file ).toFile();
        BufferedReader reader = new BufferedReader( new FileReader( initFile ) );
        String[] commands = reader.lines().filter( line -> !line.matches( "^\\s*//" ) ).collect( Collectors.joining( "\n" ) ).split( ";\\s*\n" );
        reader.close();
        for ( String command : commands )
        {
            if ( commandIsValid( command ) )
            {
                log.info( "Executing security initialization command: " + command );
                Result result = tx.execute( command );
                result.accept( new LoggingResultVisitor( result.columns() ) );
                result.close();
            }
            else
            {
                log.warn( "Ignoring invalid security initialization command: " + command );
            }
        }
    }

    private boolean commandIsValid( String command )
    {
        return !command.matches( "^\\s*.*//" ) // Ignore comments
                && command.replaceAll( "\n", " " ).matches( "^\\s*\\w+.*" ); // Ignore blank lines
    }

    private class LoggingResultVisitor implements Result.ResultVisitor<RuntimeException>
    {
        private List<String> columns;

        private LoggingResultVisitor( List<String> columns )
        {
            this.columns = columns;
        }

        @Override
        public boolean visit( Result.ResultRow row )
        {
            StringBuilder sb = new StringBuilder();
            for ( String column : columns )
            {
                if ( sb.length() > 0 )
                {
                    sb.append( ", " );
                }
                sb.append( column ).append( ":" ).append( row.get( column ).toString() );
            }
            log.info( "Result: " + sb.toString() );
            return true;
        }
    }
}
