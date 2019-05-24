#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.",".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
.$common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Uninstall-Neo4jServer" {
    # Setup mocking environment
    #  Mock Java environment
    $javaHome = global:New-MockJavaHome
    Mock Get-Neo4jEnv { $javaHome } -ParameterFilter { $Name -eq 'JAVA_HOME' }
    Mock Set-Neo4jEnv {}
    Mock Test-Path { $false } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment'
    }
    Mock Get-ItemProperty { $null } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment*'
    }
    # Mock service and process handlers
    Mock Get-Service { @{ 'State' = 'Running' } } -ParameterFilter { $Name = $global:mockServiceName }
    Mock Get-JavaVersion { @{ 'isValid' = $true; 'isJava11' = $true } }
    Mock Start-Process { throw "Should not call Start-Process mock" }
    Mock Invoke-ExternalCommand { throw "Should not call Invoke-ExternalCommand mock" }
    Mock Stop-Service { $true } -ParameterFilter { $Name -eq $global:mockServiceName }

    Context "Missing service name in configuration files" {
      $serverObject = global:New-MockNeo4jInstall -WindowsService $null

      It "throws error for missing service name in configuration file" {
        { Uninstall-Neo4jServer -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Windows service does not exist" {
      Mock Get-Service -Verifiable { $null }

      $serverObject = global:New-MockNeo4jInstall

      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Uninstall windows service successfully" {
      Mock Invoke-ExternalCommand { @{ 'exitCode' = 0 } }

      $serverObject = global:New-MockNeo4jInstall

      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }
    }

    Context "During uninstall, does not stop service if already stopped" {
      Mock Get-Service { @{ 'Status' = 'Stopped' } }
      Mock Invoke-ExternalCommand { @{ 'exitCode' = 0 } }

      $serverObject = global:New-MockNeo4jInstall

      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }

      It "does not call Stop-Service" {
        Assert-MockCalled Stop-Service -Times 0
      }
    }
  }
}
