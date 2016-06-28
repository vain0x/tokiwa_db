namespace TokiwaDb.Core.Test

open Chessie.ErrorHandling
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module TableTest =
  let testDb = new MemoryDatabase("testDb")
  let storage = testDb.Storage
  let rev = testDb.Transaction.RevisionServer
  
  let testData =
    [
      [| String "Miku"; Int 16L |]
      [| String "Yukari"; Int 18L |]
      [| String "Kaito"; Int 20L |]
    ]

  let persons =
    let schema =
      { TableSchema.empty "persons" with
          Fields =
            [|
              Field.string "name"
              Field.int "age"
            |]
      }
    in
      testDb.CreateTable(schema)

  let insertTest =
    test {
      do! persons.Insert(testData |> List.toArray)
        |> Trial.returnOrFail
        |> assertEquals [| 0L; 1L; 2L |]
      let expected =
        testData
        |> List.mapi (fun i record -> Array.append [| Int (int64 i) |] record)
      let actual =
        persons.Relation(rev.Current).RecordPointers
        |> Seq.map storage.Derefer
        |> Seq.toList
      do! actual |> assertEquals expected
    }
    
  let insertFailureTest =
    test {
      // Wrong count of fields.
      do!
        persons.Insert([| [||] |])
        |> assertSatisfies (function | Fail [Error.WrongRecordType _] -> true | _ -> false)
      // Type mismatch.
      do!
        persons.Insert([| [| String "Iroha"; String "AGE" |] |])
        |> assertSatisfies (function | Fail [Error.WrongRecordType _] -> true | _ -> false)
    }

  let recordByIdTest =
    test {
      do! persons.RecordById(0L)
        |> assertSatisfies Option.isSome
    }

  let toSeqTest =
    let body (expected, (recordPointer: Mortal<RecordPointer>)) =
      test {
        do! storage.Derefer(recordPointer.Value).[1..] |> assertEquals expected
      }
    in
      parameterize {
        source (persons.RecordPointers |> Seq.toList |> List.zip testData)
        run body
      }

  let removeTest =
    test {
      let previousRevisionId = rev.Current
      // Remove Yukari.
      do! persons.Remove([| 1L |]) |> assertEquals (Trial.pass ())
      let actual = persons.Relation(testDb.CurrentRevisionId).RecordPointers |> Seq.toList
      do! actual |> List.length |> assertEquals 2
      /// And the previous version is still available.
      let actual = persons.Relation(previousRevisionId).RecordPointers |> Seq.toList
      do! actual |> List.length |> assertEquals 3
    }

  let removeFailureTest =
    test {
      let containsInvalidId =
        function
        | Fail ([Error.InvalidRecordId _]) -> true
        | _ -> false
      do! persons.Remove([| -1L |]) |> assertSatisfies containsInvalidId
      do! persons.Remove([| 9L |]) |> assertSatisfies containsInvalidId
    }

  let dropTest =
    test {
      let () = persons.Drop()
      do! persons |> assertSatisfies (Mortal.isAliveAt testDb.CurrentRevisionId >> not)
      // Try to insert into/remove from dropped table should cause an error.
      let assertCausesTableAlreadyDroppedError result =
        result |> assertSatisfies (function | Fail [Error.TableAlreadyDropped _] -> true | _ -> false)
      do! persons.Insert([| [| String "Len"; Int 14L |] |])
        |> assertCausesTableAlreadyDroppedError
      do! persons.Remove([| 0L |])
        |> assertCausesTableAlreadyDroppedError
    }

  let ``Insert/Remove to table with an index`` =
    test {
      let schema =
        { TableSchema.empty "persons2" with
            Fields =
              [| Field.string "name"; Field.int "age" |]
            Indexes =
              [| HashTableIndexSchema [| 1 |] |]
        }
      // Create a table with index in "name" column.
      // NOTE: The first column (with index 0) is "id".
      let persons2 =
        testDb.CreateTable(schema)
      let _ =
        persons2.Insert
          ([|
            [| String "Miku"; Int 16L |]
            [| String "Yukari"; Int 18L |]
          |])
      do! persons2.Indexes.Length |> assertEquals 1
      let index       = persons2.Indexes.[0]
      do! index.TryFind(storage.Store([| String "Miku" |])) |> assertEquals (Some 0L)
      // Then remove Miku.
      let _ =
        persons2.Remove([| 0L |])
      do! index.TryFind(storage.Store([| String "Miku"  |])) |> assertEquals None
      // Duplication error test. (With records already written.)
      let actual =
        persons2.Insert([| [| String "Yukari"; Int 99L |] |])
        |> (function | Fail ([Error.DuplicatedRecord _]) -> true | _ -> false)
      // Duplication error test. (With records in the argument.)
      do!
        persons2.Insert
          ([|
            [| String "Len"; Int 14L |]
            [| String "Len"; Int 15L |]
          |])
        |> assertSatisfies (function | Fail [Error.DuplicatedRecord _] -> true | _ -> false)
    }

  let performTest =
    test {
      let schema =
        { TableSchema.empty "songs" with
            Fields = [| Field.string "title"; Field.string "composer" |]
        }
      let songs =
        testDb.CreateTable(schema)
      let operations =
        [|
          InsertRecords (songs.Id,
            [|
              [| String "Ura Omote Lovers"; String "wowaka" |]
              [| String "Rollin' Girl"; String "wowaka" |]
            |] |> Array.map (fun rp -> Array.append [| PInt 0L |] (storage.Store(rp)))
            )
          RemoveRecords (songs.Id, [| 0L |])
        |]
      let () = testDb.Perform(operations)
      // We need to bump up the revision number
      // because inserted records are alive only after rev.Next.
      let _ = rev.Increase()
      do! songs.Relation(rev.Current).RecordPointers |> Seq.length |> assertEquals 1
    }

  let transactionTest =
    test {
      let schema =
        { TableSchema.empty "items" with
            Fields = [| Field.string "vocaloid"; Field.string "item" |]
            Indexes = [| HashTableIndexSchema [| 1 |] |]
        }
      let items = testDb.CreateTable(schema)
      // Commit test.
      let _ =
        testDb |> Database.transact (fun () ->
          items.Insert
            ([|
              [| String "Miku"; String "Green onions" |]
              [| String "Yukari"; String "Chainsaws" |]
            |])
          )
      do! items.RecordPointers |> Seq.length |> assertEquals 2

      // Rollback test.
      let transaction = testDb.Transaction
      let () = transaction.Begin()
      let _ =
        items.Remove([|0L|])
      let assertThatRemoveHasNotBeenPerformed () =
        items.RecordPointers |> Seq.head |> assertSatisfies (Mortal.isAliveAt rev.Current)
      do! assertThatRemoveHasNotBeenPerformed ()
      let () =
        transaction.Rollback()
      do! assertThatRemoveHasNotBeenPerformed ()

      // Nested transaction.
      let () = transaction.Begin()
      let _ =
        items.Insert([| [| String "Kaito"; String "Ices" |] |])
      let () = transaction.Begin()
      let _ =
        items.Remove([|0L|])
      let () =
        // Rollback the internal transaction, which discards the remove but not the insert.
        transaction.Rollback()
      let () =
        transaction.Commit()
      do! assertThatRemoveHasNotBeenPerformed ()
      do! items.RecordPointers |> Seq.length |> assertEquals 3

      // Inserting a duplicated record should return an error.
      let () = transaction.Begin()
      do! items.Insert([| [| String "Len"; String "Bananas" |] |])
        |> Trial.returnOrFail
        |> assertEquals [| 3L |]
      do! items.Insert([| [| String "Len"; String "Headphones" |] |])
        |> assertSatisfies (function | Fail ([Error.DuplicatedRecord _]) -> true | _ -> false)
      // Insert should return the actual record IDs of inserted records.
      do! items.Insert([| [| String "Rin"; String "Oranges" |] |])
        |> Trial.returnOrFail
        |> assertEquals [| 4L |]
      let () = transaction.Rollback()

      // Drop test.
      let () = transaction.Begin()
      let () = items.Drop()
      // Dropped table can be dropped. Just nothing happens.
      let () = items.Drop() 
      // Dropped table can't be modified.
      do! items.Remove([|0L|])
        |> assertSatisfies (function | Fail [Error.TableAlreadyDropped _] -> true | _ -> false)
      let () = transaction.Rollback()
      return ()
    }
