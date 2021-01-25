/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import com.neo4j.server.security.enterprise.auth.plugin.api.AuthenticationException;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;

import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

public class LdapGroupHasUsersAuthPlugin extends AuthPlugin.Adapter
{
    private static final String GROUP_SEARCH_BASE = "ou=groups,dc=example,dc=com";
    private static final String GROUP_SEARCH_FILTER = "(&(objectClass=posixGroup)(memberUid={0}))";
    public static final String GROUP_ID = "gidNumber";

    @Override
    public String name()
    {
        return "ldap-alternative-groups";
    }

    @Override
    public AuthInfo authenticateAndAuthorize( AuthToken authToken ) throws AuthenticationException
    {
        try
        {
            String username = authToken.principal();
            char[] password = authToken.credentials();
            Map<String,Object> parameters = authToken.parameters();

            LdapContext ctx = authenticate( username, password, parameters );
            Set<String> roles = authorize( ctx, username );

            return AuthInfo.of( username, roles );
        }
        catch ( NamingException e )
        {
            throw new AuthenticationException( e.getMessage() );
        }
    }

    private static LdapContext authenticate( String username, char[] password, Map<String,Object> parameters ) throws NamingException
    {
        long port = (long) parameters.get( "port" );

        Hashtable<String,Object> env = new Hashtable<>();
        env.put( Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory" );
        env.put( Context.PROVIDER_URL, "ldap://0.0.0.0:" + port );

        env.put( Context.SECURITY_PRINCIPAL, String.format( "cn=%s,ou=users,dc=example,dc=com", username ) );
        env.put( Context.SECURITY_CREDENTIALS, password );

        return new InitialLdapContext( env, null );
    }

    private Set<String> authorize( LdapContext ctx, String username ) throws NamingException
    {
        Set<String> roleNames = new LinkedHashSet<>();

        // Setup our search controls
        SearchControls searchCtls = new SearchControls();
        searchCtls.setSearchScope( SearchControls.SUBTREE_SCOPE );
        searchCtls.setReturningAttributes( new String[]{GROUP_ID} );

        // Use a search argument to prevent potential code injection
        Object[] searchArguments = new Object[]{username};

        // Search for groups that has the user as a member
        NamingEnumeration<SearchResult> result = ctx.search( GROUP_SEARCH_BASE, GROUP_SEARCH_FILTER, searchArguments, searchCtls );

        if ( result.hasMoreElements() )
        {
            SearchResult searchResult = result.next();

            Attributes attributes = searchResult.getAttributes();
            if ( attributes != null )
            {
                NamingEnumeration<? extends Attribute> attributeEnumeration = attributes.getAll();
                while ( attributeEnumeration.hasMore() )
                {
                    Attribute attribute = attributeEnumeration.next();
                    String attributeId = attribute.getID();
                    if ( attributeId.equalsIgnoreCase( GROUP_ID ) )
                    {
                        // We found a group that the user is a member of. See if it has a role mapped to it
                        String groupId = (String) attribute.get();
                        String neo4jGroup = getNeo4jRoleForGroupId( groupId );
                        if ( neo4jGroup != null )
                        {
                            // Yay! Add it to our set of roles
                            roleNames.add( neo4jGroup );
                        }
                    }
                }
            }
        }
        return roleNames;
    }

    private String getNeo4jRoleForGroupId( String groupId )
    {
        if ( "500".equals( groupId ) )
        {
            return PredefinedRoles.READER;
        }
        if ( "501".equals( groupId ) )
        {
            return PredefinedRoles.PUBLISHER;
        }
        if ( "502".equals( groupId ) )
        {
            return PredefinedRoles.ARCHITECT;
        }
        if ( "503".equals( groupId ) )
        {
            return PredefinedRoles.ADMIN;
        }
        return null;
    }
}
