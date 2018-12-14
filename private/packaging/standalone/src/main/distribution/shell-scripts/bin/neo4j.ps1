# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#
# Module manifest for module 'Neo4j-Management'
#

try
{
    Unblock-File -Path '%~dp0Neo4j-Management\*.*' -ErrorAction 'SilentlyContinue'
}
catch
{
};

$verbose = "$args" -match "-v|-verbose"
$actualArgs = $args -notmatch "-v|-verbose"

Import-Module "$PSScriptRoot\Neo4j-Management.psd1"
Exit (Invoke-Neo4j -Verbose:$verbose -Command ($actualArgs -join ' '))
