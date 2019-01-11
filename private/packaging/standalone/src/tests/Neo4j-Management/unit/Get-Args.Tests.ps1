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
  Describe "Get-Args" {

    It "should return Verbose=false when args is empty" {
      $args = Get-Args

      $args.Verbose | Should Be $false
      $args.Args.Count | Should Be 0
      $args.ArgsAsStr | Should Be ''
    }

    It "should return Verbose=true when -v is passed" {
      $args = Get-Args @('-v','some','other','args')

      $args.Verbose | Should Be $true
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return Verbose=true when -V is passed" {
      $args = Get-Args @('some','-V','other','args')

      $args.Verbose | Should Be $true
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return verbose=true when -verbose is passed" {
      $args = Get-Args @('some','other','-verbose','args')

      $args.Verbose | Should Be $true
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return verbose=true when -Verbose is passed" {
      $args = Get-Args @('some','other','-Verbose','args')

      $args.Verbose | Should Be $true
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return verbose=true when -VeRbOsE is passed" {
      $args = Get-Args @('some','other','args','-VeRbOsE')

      $args.Verbose | Should Be $true
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return Verbose=false when no verbose argument is passed" {
      $args = Get-Args @('some','other','args')

      $args.Verbose | Should Be $false
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return Verbose=false when -verb argument is passed" {
      $args = Get-Args @('some','other','args','-verb')

      $args.Verbose | Should Be $false
      $args.Args | Should Be @('some','other','args','-verb')
      $args.ArgsAsStr | Should Be 'some other args -verb'
    }

    It "should return Verbose=false when no-verbose argument is passed" {
      $args = Get-Args @('some','other','no-verbose','args')

      $args.Verbose | Should Be $false
      $args.Args | Should Be @('some','other','no-verbose','args')
      $args.ArgsAsStr | Should Be 'some other no-verbose args'
    }
  }
}
