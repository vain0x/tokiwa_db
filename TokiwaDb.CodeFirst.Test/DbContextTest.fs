namespace TokiwaDb.CodeFirst.Test

open System
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.Core.Test
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

type TestDbContext =
  {
    Database            : Database
    Persons             : Table<Person>
    Songs               : Table<Song>
  }

module DbContextTest =
  open ModelTest

  type TestDbContextClass
      ( _database: Database
      , _persons: Table<Person>
      , _songs: Table<Song>
      )
    =
    member this.Database        = _database
    member this.Persons         = _persons
    member this.Songs           = _songs

  let isTableTypeTest =
    parameterize {
      case (typeof<Table<Person>>, true)
      case (typeof<Person>, false)
      run (testFun DbContext.isTableType)
    }

  let tablePropertiesTest =
    test {
      do! typeof<TestDbContextClass>
        |> DbContext.tableProperties
        |> Array.map (fun pi -> pi.Name)
        |> assertEquals [| "Persons"; "Songs" |]
    }

  let modelTypesTest =
    test {
      do! typeof<TestDbContextClass>
        |> DbContext.modelTypes
        |> assertEquals [| typeof<Person>; typeof<Song> |]
    }

  let testCreateInstance contextType =
    test {
      let createEmptyTable modelType =
        let tableType   = typedefof<EmptyTable<_>>.MakeGenericType([| modelType |])
        in Activator.CreateInstance(tableType)
      use db = new EmptyDatabase()
      let instance = contextType |> DbContext.construct db createEmptyTable
      return ()
    }

  let createInstanceTest =
    test {
      do! testCreateInstance typeof<TestDbContextClass>
      do! testCreateInstance typeof<TestDbContext>
    }
