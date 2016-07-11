namespace TokiwaDb.CodeFirst.Test

open TokiwaDb.CodeFirst
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection

module OrmTableTest =
  open OrmDatabaseTest

  let insertTest =
    test {
      let db = testDb ()
      let person = Person(Name = "Miku", Age = 16L)
      let persons = db.Persons
      persons.Insert(person)
      do! person.Id |> assertEquals 0L
    }

  let seedDb () =
    let db = testDb ()
    let persons = db.Persons
    persons.Insert(Person(Name = "Miku", Age = 16L))
    persons.Insert(Person(Name = "Yukari", Age = 18L))
    db

  let allItemsTest =
    test {
      let db = seedDb ()
      let persons = db.Persons
      do! persons.AllItems
        |> Seq.toArray
        |> Array.map (fun person -> person.Name)
        |> assertEquals [| "Miku"; "Yukari" |]
    }

  let removeTest =
    test {
      let db = seedDb ()
      let persons = db.Persons
      persons.Remove(0L)
      do! persons.Items
        |> Seq.map (fun person -> person.Name)
        |> Seq.toArray
        |> assertEquals [| "Yukari" |]
    }
