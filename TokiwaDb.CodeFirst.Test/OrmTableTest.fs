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
      let persons = db.Table<Person>()
      persons.Insert(person)
      do! person.Id |> assertEquals 0L
    }

  let seedDb () =
    let db = testDb ()
    let persons = db.Table<Person>()
    persons.Insert(Person(Name = "Miku", Age = 16L))
    persons.Insert(Person(Name = "Yukari", Age = 18L))
    db

  let itemsTest =
    test {
      let db = seedDb ()
      let persons = db.Table<Person>()
      do! persons.Items
        |> Seq.toArray
        |> Array.map (fun person -> person.Name)
        |> assertEquals [| "Miku"; "Yukari" |]
    }
