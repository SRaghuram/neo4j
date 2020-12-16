/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.configuration.SecurityInternalSettings;
import com.neo4j.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.apache.shiro.realm.ldap.LdapContextFactory;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.matchers.Any;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;

import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.ShiroAuthToken;

import static com.neo4j.configuration.SecuritySettings.ldap_authorization_group_to_role_mapping;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

class LdapRealmTest
{
    Config config = mock( Config.class );
    private SecurityLog securityLog = mock( SecurityLog.class );
    private SecureHasher secureHasher = new SecureHasher();
    private LogProvider logProvider = NullLogProvider.getInstance();

    @BeforeEach
    void setUp()
    {
        // Some dummy settings to pass validation
        when( config.get( SecuritySettings.ldap_authorization_user_search_base ) )
                .thenReturn( "dc=example,dc=com" );
        when( config.get( SecuritySettings.ldap_authorization_group_membership_attribute_names ) )
                .thenReturn( singletonList( "memberOf" ) );

        when( config.get( SecuritySettings.ldap_authentication_cache_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_connection_timeout ) ).thenReturn( Duration.ofSeconds( 1 ) );
        when( config.get( SecuritySettings.ldap_read_timeout ) ).thenReturn( Duration.ofSeconds( 1 ) );
        when( config.get( SecurityInternalSettings.ldap_authorization_connection_pooling ) ).thenReturn( true );
        when( config.get( SecuritySettings.ldap_authentication_use_attribute ) ).thenReturn( false );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToBeNull()
    {
        when( config.get( ldap_authorization_group_to_role_mapping ) ).thenReturn( null );

        new LdapRealm( config, logProvider, securityLog, secureHasher, true, true );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToBeEmpty()
    {
        when( config.get( ldap_authorization_group_to_role_mapping ) ).thenReturn( "" );

        new LdapRealm( config, logProvider, securityLog, secureHasher,true, true );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveMultipleRoles()
    {
        when( config.get( ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "group=role1,role2,role3" );

        LdapRealm realm = new LdapRealm( config, logProvider, securityLog, secureHasher,true, true );

        assertThat( realm.getGroupToRoleMapping().get( "group" ) ).isEqualTo( asList( "role1", "role2", "role3" ) );
        assertThat( realm.getGroupToRoleMapping().size() ).isEqualTo( 1 );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveMultipleGroups()
    {
        when( config.get( ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "group1=role1;group2=role2,role3;group3=role4" );

        LdapRealm realm = new LdapRealm( config, logProvider, securityLog, secureHasher,true, true );

        assertThat( realm.getGroupToRoleMapping().keySet() ).isEqualTo( new TreeSet<>( asList( "group1", "group2", "group3" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group1" ) ).isEqualTo( singletonList( "role1" ) );
        assertThat( realm.getGroupToRoleMapping().get( "group2" ) ).isEqualTo( asList( "role2", "role3" ) );
        assertThat( realm.getGroupToRoleMapping().get( "group3" ) ).isEqualTo( singletonList( "role4" ) );
        assertThat( realm.getGroupToRoleMapping().size() ).isEqualTo( 3 );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveQuotedKeysAndWhitespaces()
    {
        when( config.get( ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "'group1' = role1;\t \"group2\"\n=\t role2,role3 ;  gr oup3= role4\n ;'group4 '= ; g =r" );

        LdapRealm realm = new LdapRealm( config, logProvider, securityLog, secureHasher,true, true );

        assertThat( realm.getGroupToRoleMapping().keySet() ).isEqualTo( new TreeSet<>( asList( "group1", "group2", "gr oup3", "group4 ", "g" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group1" ) ).isEqualTo( singletonList( "role1" ) );
        assertThat( realm.getGroupToRoleMapping().get( "group2" ) ).isEqualTo( asList( "role2", "role3" ) );
        assertThat( realm.getGroupToRoleMapping().get( "gr oup3" ) ).isEqualTo( singletonList( "role4" ) );
        assertThat( realm.getGroupToRoleMapping().get( "group4 " ) ).isEqualTo( Collections.emptyList() );
        assertThat( realm.getGroupToRoleMapping().get( "g" ) ).isEqualTo( singletonList( "r" ) );
        assertThat( realm.getGroupToRoleMapping().size() ).isEqualTo( 5 );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveTrailingSemicolons()
    {
        when( config.get( ldap_authorization_group_to_role_mapping ) ).thenReturn( "group=role;;" );

        LdapRealm realm = new LdapRealm( config, logProvider, securityLog, secureHasher,true, true );

        assertThat( realm.getGroupToRoleMapping().get( "group" ) ).isEqualTo( singletonList( "role" ) );
        assertThat( realm.getGroupToRoleMapping().size() ).isEqualTo( 1 );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveTrailingCommas()
    {
        when( config.get( ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "group=role1,role2,role3,,," );

        LdapRealm realm = new LdapRealm( config, logProvider, securityLog, secureHasher, true, true );

        assertThat( realm.getGroupToRoleMapping().keySet() ).isEqualTo( Stream.of( "group" ).collect( Collectors.toSet() ) );
        assertThat( realm.getGroupToRoleMapping().get( "group" ) ).isEqualTo( asList( "role1", "role2", "role3" ) );
        assertThat( realm.getGroupToRoleMapping().size() ).isEqualTo( 1 );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveNoRoles()
    {
        when( config.get( ldap_authorization_group_to_role_mapping ) ).thenReturn( "group=," );

        LdapRealm realm = new LdapRealm( config, logProvider, securityLog, secureHasher, true, true );

        assertThat( realm.getGroupToRoleMapping().get( "group" ).size() ).isEqualTo( 0 );
        assertThat( realm.getGroupToRoleMapping().size() ).isEqualTo( 1 );
    }

    @Test
    void groupToRoleMappingShouldNotBeAbleToHaveInvalidFormat()
    {
        when( config.get( ldap_authorization_group_to_role_mapping ) ).thenReturn( "group" );

        var e = assertThrows( IllegalArgumentException.class, () -> new LdapRealm( config, logProvider, securityLog, secureHasher, true, true ) );
        assertThat( e.getMessage() ).contains( "wrong number of fields" );
    }

    @Test
    void groupToRoleMappingShouldNotBeAbleToHaveEmptyGroupName()
    {
        when( config.get( ldap_authorization_group_to_role_mapping ) ).thenReturn( "=role" );

        var e = assertThrows( IllegalArgumentException.class, () -> new LdapRealm( config, logProvider, securityLog, secureHasher, true, true ) );
        assertThat( e.getMessage() ).contains( "wrong number of fields" );
    }

    @Test
    void groupComparisonShouldBeCaseInsensitive()
    {
        when( config.get( ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "GrouP=role1,role2,role3" );

        LdapRealm realm = new LdapRealm( config, logProvider, securityLog, secureHasher, true, true );

        assertThat( realm.getGroupToRoleMapping().get( "group" ) ).isEqualTo( asList( "role1", "role2", "role3" ) );
        assertThat( realm.getGroupToRoleMapping().size() ).isEqualTo( 1 );
    }

    @Test
    void searchAttributeShouldBeAbleToContainAllValidCharacters()
    {
        // Allowed to contain A-Z, a-z, 0-9 and -
        Config conf = Config.newBuilder().set( SecuritySettings.ldap_authentication_user_search_attribute_name, "0-uSer123-Dn" ).build();
        assertThat( conf.get( SecuritySettings.ldap_authentication_user_search_attribute_name ) ).isEqualTo( "0-uSer123-Dn" );
    }

    @Test
    void searchAttributeShouldNotBeAbleToContainIllegalCharacters()
    {
         var e = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                 .set( SecuritySettings.ldap_authentication_user_search_attribute_name, "user_dn" ).build() );

         assertThat( e.getMessage() ).contains( "has to be a valid LDAP attribute name, only containing letters [A-Za-z], digits [0-9] and hyphens [-]" );
    }

    @Test
    void shouldWarnAboutUserSearchFilterWithoutArgument() throws Exception
    {
        when( config.get( SecuritySettings.ldap_authorization_user_search_filter ) ).thenReturn( "" );

        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        when( ldapContext.search( anyString(), anyString(), any(), any() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( false );

        makeAndInit();

        verify( securityLog ).warn( contains( "LDAP user search filter does not contain the argument placeholder {0}" ) );
    }

    @Test
    void shouldWarnAboutUserSearchBaseBeingEmpty() throws Exception
    {
        when( config.get( SecuritySettings.ldap_authorization_user_search_base ) ).thenReturn( "" );

        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        when( ldapContext.search( anyString(), anyString(), any(), any() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( false );

        assertThatThrownBy( this::makeAndInit ).isInstanceOf( IllegalArgumentException.class )
                .hasMessage( "Illegal LDAP user search settings, see security log for details." );

        verify( securityLog ).error( contains( "LDAP user search base is empty." ) );
    }

    @Test
    void shouldWarnAboutGroupMembershipsBeingEmpty() throws Exception
    {
        when( config.get( SecuritySettings.ldap_authorization_group_membership_attribute_names ) )
                .thenReturn( Collections.emptyList() );

        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        when( ldapContext.search( anyString(), anyString(), any(), any() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( false );

        assertThatThrownBy( this::makeAndInit ).isInstanceOf( IllegalArgumentException.class )
                .hasMessage( "Illegal LDAP user search settings, see security log for details." );

        verify( securityLog ).error( contains( "LDAP group membership attribute names are empty. " +
                "Authorization will not be possible." ) );
    }

    @Test
    void shouldWarnAboutAmbiguousUserSearch() throws NamingException
    {
        when( config.get( SecuritySettings.ldap_authorization_user_search_filter ) ).thenReturn( "{0}" );

        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        SearchResult searchResult = mock( SearchResult.class );
        when( ldapContext.search( anyString(), anyString(), any(), any() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( true );
        when( result.next() ).thenReturn( searchResult );
        when( searchResult.toString() ).thenReturn( "<ldap search result>" );

        LdapRealm realm = new LdapRealm( config, logProvider, securityLog, secureHasher, true, true );
        realm.findRoleNamesForUser( "username", ldapContext );

        verify( securityLog ).warn( contains( "LDAP user search for user principal 'username' is ambiguous" ) );
    }

    @Test
    void shouldAllowMultipleGroupMembershipAttributes() throws NamingException
    {
        when( config.get( SecuritySettings.ldap_authorization_user_search_filter ) ).thenReturn( "{0}" );
        when( config.get( SecuritySettings.ldap_authorization_group_membership_attribute_names ) )
                .thenReturn( asList( "attr0", "attr1", "attr2" ) );
        when( config.get( ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "group1=role1;group2=role2,role3" );

        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        SearchResult searchResult = mock( SearchResult.class );
        Attributes attributes = mock( Attributes.class );
        Attribute attribute1 = mock( Attribute.class );
        Attribute attribute2 = mock( Attribute.class );
        Attribute attribute3 = mock( Attribute.class );
        NamingEnumeration attributeEnumeration = mock( NamingEnumeration.class );
        NamingEnumeration groupEnumeration1 = mock( NamingEnumeration.class );
        NamingEnumeration groupEnumeration2 = mock( NamingEnumeration.class );
        NamingEnumeration groupEnumeration3 = mock( NamingEnumeration.class );

        // Mock ldap search result "attr1" contains "group1" and "attr2" contains "group2" (a bit brittle...)
        // "attr0" is non-existing and should have no effect
        when( ldapContext.search( anyString(), anyString(), any(), any() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( true, false );
        when( result.next() ).thenReturn( searchResult );
        when( searchResult.getAttributes() ).thenReturn( attributes );
        when( attributes.getAll() ).thenReturn( attributeEnumeration );
        when( attributeEnumeration.hasMore() ).thenReturn( true, true, false );
        when( attributeEnumeration.next() ).thenReturn( attribute1, attribute2, attribute3 );

        when( attribute1.getID() ).thenReturn( "attr1" ); // This attribute should yield role1
        when( attribute1.getAll() ).thenReturn( groupEnumeration1 );
        when( groupEnumeration1.hasMore() ).thenReturn( true, false );
        when( groupEnumeration1.next() ).thenReturn( "group1" );

        when( attribute2.getID() ).thenReturn( "attr2" ); // This attribute should yield role2 and role3
        when( attribute2.getAll() ).thenReturn( groupEnumeration2 );
        when( groupEnumeration2.hasMore() ).thenReturn( true, false );
        when( groupEnumeration2.next() ).thenReturn( "group2" );

        when( attribute3.getID() ).thenReturn( "attr3" ); // This attribute should have no effect
        when( attribute3.getAll() ).thenReturn( groupEnumeration3 );
        when( groupEnumeration3.hasMore() ).thenReturn( true, false );
        when( groupEnumeration3.next() ).thenReturn( "groupWithNoRole" );

        // When
        LdapRealm realm = new LdapRealm( config, logProvider, securityLog, secureHasher, true, true );
        Set<String> roles = realm.findRoleNamesForUser( "username", ldapContext );

        // Then
        assertThat( roles ).contains( "role1", "role2", "role3" );
    }

    @Test
    void shouldLogSuccessfulAuthenticationQueries() throws NamingException
    {
        // Given
        when( config.get( SecuritySettings.ldap_use_starttls ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_authorization_use_system_account ) ).thenReturn( true );

        LdapRealm realm = new TestLdapRealm( config, securityLog, false );
        JndiLdapContextFactory jndiLdapContectFactory = mock( JndiLdapContextFactory.class );
        when( jndiLdapContectFactory.getUrl() ).thenReturn( "ldap://myserver.org:12345" );
        when( jndiLdapContectFactory.getLdapContext( Any.ANY, Any.ANY ) ).thenReturn( null );

        // When
        realm.queryForAuthenticationInfo( new ShiroAuthToken( map( "principal", "olivia", "credentials", "123" ) ),
                jndiLdapContectFactory );

        // Then
        verify( securityLog ).debug( contains( "{LdapRealm}: Authenticated user 'olivia' against 'ldap://myserver.org:12345'" ) );
    }

    @Test
    void shouldLogSuccessfulAuthenticationQueriesUsingStartTLS() throws NamingException
    {
        // Given
        when( config.get( SecuritySettings.ldap_use_starttls ) ).thenReturn( true );

        LdapRealm realm = new TestLdapRealm( config, securityLog, false );
        JndiLdapContextFactory jndiLdapContectFactory = mock( JndiLdapContextFactory.class );
        when( jndiLdapContectFactory.getUrl() ).thenReturn( "ldap://myserver.org:12345" );

        // When
        realm.queryForAuthenticationInfo( new ShiroAuthToken( map( "principal", "olivia", "credentials", "123" ) ),
                jndiLdapContectFactory );

        // Then
        verify( securityLog ).debug( contains(
                "{LdapRealm}: Authenticated user 'olivia' against 'ldap://myserver.org:12345' using StartTLS" ) );
    }

    @Test
    void shouldLogFailedAuthenticationQueries()
    {
        // Given
        when( config.get( SecuritySettings.ldap_use_starttls ) ).thenReturn( true );

        LdapRealm realm = new TestLdapRealm( config, securityLog, true );
        JndiLdapContextFactory jndiLdapContectFactory = mock( JndiLdapContextFactory.class );
        when( jndiLdapContectFactory.getUrl() ).thenReturn( "ldap://myserver.org:12345" );

        // When
        assertThrows( NamingException.class,
                () -> realm.queryForAuthenticationInfo( new ShiroAuthToken( map( "principal", "olivia", "credentials", "123" ) ), jndiLdapContectFactory ) );
    }

    @Test
    void shouldLogSuccessfulAuthorizationQueries()
    {
        // Given
        when( config.get( SecuritySettings.ldap_use_starttls ) ).thenReturn( true );

        LdapRealm realm = new TestLdapRealm( config, securityLog, false );
        JndiLdapContextFactory jndiLdapContectFactory = mock( JndiLdapContextFactory.class );
        when( jndiLdapContectFactory.getUrl() ).thenReturn( "ldap://myserver.org:12345" );

        // When
        realm.doGetAuthorizationInfo( new SimplePrincipalCollection( "olivia", "LdapRealm" ) );

        // Then
        verify( securityLog ).debug( contains( "{LdapRealm}: Queried for authorization info for user 'olivia'" ) );
    }

    @Test
    void shouldLogFailedAuthorizationQueries()
    {
        // Given
        when( config.get( SecuritySettings.ldap_use_starttls ) ).thenReturn( true );

        LdapRealm realm = new TestLdapRealm( config, securityLog, true );
        JndiLdapContextFactory jndiLdapContectFactory = mock( JndiLdapContextFactory.class );
        when( jndiLdapContectFactory.getUrl() ).thenReturn( "ldap://myserver.org:12345" );

        // When
        AuthorizationInfo info = realm.doGetAuthorizationInfo( new SimplePrincipalCollection( "olivia", "LdapRealm" ) );

        // Then
        assertNull( info );
        verify( securityLog ).warn( contains( "{LdapRealm}: Failed to get authorization info: " +
                "'LDAP naming error while attempting to retrieve authorization for user [olivia].'" +
                " caused by 'Simulated failure'"
        ) );
    }

    private class TestLdapRealm extends LdapRealm
    {

        private boolean failAuth;

        TestLdapRealm( Config config, SecurityLog securityLog, boolean failAuth )
        {
            super( config, logProvider, securityLog, secureHasher, true, true );
            this.failAuth = failAuth;
        }

        @Override
        protected AuthenticationInfo queryForAuthenticationInfoUsingStartTls( AuthenticationToken token,
                LdapContextFactory ldapContextFactory ) throws NamingException
        {
            if ( failAuth )
            {
                throw new NamingException( "Simulated failure" );
            }
            return new SimpleAuthenticationInfo( "olivia", "123", "basic" );
        }

        @Override
        protected AuthorizationInfo queryForAuthorizationInfo( PrincipalCollection principals,
                LdapContextFactory ldapContextFactory ) throws NamingException
        {
            if ( failAuth )
            {
                throw new NamingException( "Simulated failure" );
            }
            return new SimpleAuthorizationInfo();
        }
    }

    private void makeAndInit()
    {
        try
        {
            LdapRealm realm = new LdapRealm( config, logProvider, securityLog, secureHasher, true, true );
            realm.initialize();
        }
        catch ( Exception e )
        {
            throw e;
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( t );
        }
    }
}
