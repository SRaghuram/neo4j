/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.configuration;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.DocumentedDefaultValue;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.configuration.Settings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.web.JettyThreadCalculator;

import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.Settings.BOOLEAN;
import static org.neo4j.configuration.Settings.BYTES;
import static org.neo4j.configuration.Settings.DURATION;
import static org.neo4j.configuration.Settings.EMPTY;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.configuration.Settings.INTEGER;
import static org.neo4j.configuration.Settings.NORMALIZED_RELATIVE_URI;
import static org.neo4j.configuration.Settings.NO_DEFAULT;
import static org.neo4j.configuration.Settings.PATH;
import static org.neo4j.configuration.Settings.STRING;
import static org.neo4j.configuration.Settings.STRING_LIST;
import static org.neo4j.configuration.Settings.buildSetting;
import static org.neo4j.configuration.Settings.derivedSetting;
import static org.neo4j.configuration.Settings.range;
import static org.neo4j.configuration.Settings.setting;
import static org.neo4j.configuration.ssl.LegacySslPolicyConfig.LEGACY_POLICY_NAME;

@Description( "Settings used by the server configuration" )
@ServiceProvider
public class ServerSettings implements LoadableConfig
{
    @Description( "Maximum request header size" )
    @Internal
    public static final Setting<Integer> maximum_request_header_size =
            setting( "unsupported.dbms.max_http_request_header_size", INTEGER, "20480" );

    @Description( "Maximum response header size" )
    @Internal
    public static final Setting<Integer> maximum_response_header_size =
            setting( "unsupported.dbms.max_http_response_header_size", INTEGER, "20480" );

    @Description( "Comma-separated list of custom security rules for Neo4j to use." )
    public static final Setting<List<String>> security_rules =
            setting( "dbms.security.http_authorization_classes", STRING_LIST, EMPTY );

    @Description( "Number of Neo4j worker threads. This setting is only valid for REST, and does not influence bolt-server. " +
            "It sets the amount of worker threads for the Jetty server used by neo4j-server. " +
            "This option can be tuned when you plan to execute multiple, concurrent REST requests, " +
            "with the aim of getting more throughput from the database. " +
            "Your OS might enforce a lower limit than the maximum value specified here." )
    @DocumentedDefaultValue( "Number of available processors, or 500 for machines which have more than 500 processors." )
    public static final Setting<Integer> webserver_max_threads = buildSetting( "dbms.threads.worker_count", INTEGER,
            "" + Math.min( Runtime.getRuntime().availableProcessors(), 500 ) ).constraint(
            range( 1, JettyThreadCalculator.MAX_THREADS ) ).build();

    @Description( "If execution time limiting is enabled in the database, this configures the maximum request execution time. " +
            "Please use dbms.transaction.timeout instead." )
    @Internal
    @Deprecated
    public static final Setting<Duration> webserver_limit_execution_time = setting( "unsupported.dbms" +
            ".executiontime_limit.time", DURATION, NO_DEFAULT );

    @Description( "Comma-separated list of <classname>=<mount point> for unmanaged extensions." )
    public static final Setting<List<ThirdPartyJaxRsPackage>> third_party_packages = setting( "dbms.unmanaged_extension_classes",
            new Function<String, List<ThirdPartyJaxRsPackage>>()
            {
                @Override
                public List<ThirdPartyJaxRsPackage> apply( String value )
                {
                    String[] list = value.split( Settings.SEPARATOR );
                    List<ThirdPartyJaxRsPackage> result = new ArrayList<>();
                    for ( String item : list )
                    {
                        item = item.trim();
                        if ( !item.equals( "" ) )
                        {
                            result.add( createThirdPartyJaxRsPackage( item ) );
                        }
                    }
                    return result;
                }

                @Override
                public String toString()
                {
                    return "a comma-separated list of <classname>=<mount point> strings";
                }

                private ThirdPartyJaxRsPackage createThirdPartyJaxRsPackage( String packageAndMountpoint )
                {
                    String[] parts = packageAndMountpoint.split( "=" );
                    if ( parts.length != 2 )
                    {
                        throw new IllegalArgumentException( "config for " + ServerSettings.third_party_packages.name()
                                + " is wrong: " + packageAndMountpoint );
                    }
                    String pkg = parts[0];
                    String mountPoint = parts[1];
                    return new ThirdPartyJaxRsPackage( pkg, mountPoint );
                }
            },
            EMPTY );

    @Description( "Value of the Access-Control-Allow-Origin header sent over any HTTP or HTTPS " +
            "connector. This defaults to '*', which allows broadest compatibility. Note " +
            "that any URI provided here limits HTTP/HTTPS access to that URI only." )
    public static final Setting<String> http_access_control_allow_origin =
            setting( "dbms.security.http_access_control_allow_origin", STRING, "*" );

    @Description( "Enable HTTP request logging." )
    public static final Setting<Boolean> http_logging_enabled = setting( "dbms.logs.http.enabled", BOOLEAN, FALSE );

    @Description( "Path to HTTP request log." )
    public static final Setting<File> http_log_path =
            derivedSetting( "dbms.logs.http.path", logs_directory, logs -> new File( logs, "http.log" ),
                    PATH );

    @Description( "Number of HTTP logs to keep." )
    public static final Setting<Integer> http_logging_rotation_keep_number =
            setting( "dbms.logs.http.rotation.keep_number", INTEGER, "5" );

    @Description( "Size of each HTTP log that is kept." )
    public static final Setting<Long> http_logging_rotation_size = buildSetting( "dbms.logs.http.rotation.size", BYTES,
            "20m" ).constraint( range(0L, Long.MAX_VALUE ) ).build();

    @Description( "Timeout for idle transactions in the REST endpoint." )
    public static final Setting<Duration> transaction_idle_timeout = setting( "dbms.rest.transaction.idle_timeout",
            DURATION, "60s" );

    @Description( "Value of the HTTP Strict-Transport-Security (HSTS) response header. " +
                  "This header tells browsers that a webpage should only be accessed using HTTPS instead of HTTP. It is attached to every HTTPS response. " +
                  "Setting is not set by default so 'Strict-Transport-Security' header is not sent. " +
                  "Value is expected to contain directives like 'max-age', 'includeSubDomains' and 'preload'." )
    public static final Setting<String> http_strict_transport_security = setting( "dbms.security.http_strict_transport_security", STRING, NO_DEFAULT );

    @Internal
    @Description( "Publicly discoverable bolt:// URI to use for Neo4j Drivers wanting to access the data in this " +
            "particular database instance. Normally this is the same as the advertised address configured for the " +
            "connector, but this allows manually overriding that default." )
    @DocumentedDefaultValue(
            "Defaults to a bolt://-schemed version of the advertised address " + "of the first found bolt connector." )
    public static final Setting<URI> bolt_discoverable_address =
            setting( "unsupported.dbms.discoverable_bolt_address", Settings.URI, "" );

    @SuppressWarnings( "unused" ) // accessed from the browser
    @Description( "Commands to be run when Neo4j Browser successfully connects to this server. Separate multiple " +
                  "commands with semi-colon." )
    public static final Setting<String> browser_postConnectCmd = setting( "browser.post_connect_cmd", STRING, "" );

    @SuppressWarnings( "unused" ) // accessed from the browser
    @Description( "Whitelist of hosts for the Neo4j Browser to be allowed to fetch content from." )
    public static final Setting<String> browser_remoteContentHostnameWhitelist =
            setting( "browser.remote_content_hostname_whitelist", STRING, "guides.neo4j.com,localhost");

    @Description( "SSL policy name." )
    public static final Setting<String> ssl_policy = setting( "https.ssl_policy", STRING, LEGACY_POLICY_NAME );

    @Internal
    public static final Setting<URI> rest_api_path = setting( "unsupported.dbms.uris.rest", NORMALIZED_RELATIVE_URI, "/db" );

    @Internal
    public static final Setting<URI> management_api_path = setting( "unsupported.dbms.uris.management",
            NORMALIZED_RELATIVE_URI, "/db/manage" );

    @Internal
    public static final Setting<URI> browser_path = setting( "unsupported.dbms.uris.browser", Settings.URI, "/browser/" );

    @Internal
    public static final Setting<Boolean> wadl_enabled = setting( "unsupported.dbms.wadl_generation_enabled", BOOLEAN,
            FALSE );
}
