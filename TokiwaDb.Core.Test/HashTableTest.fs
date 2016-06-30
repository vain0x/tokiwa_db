namespace TokiwaDb.Core.Test

open System
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module HashTableTest =
  open HashTableDetail

  let hashTableElementSerializerTest =
    HashTableElementSerializer<int, string>(IntSerializer(), FixedStringSerializer())
    |> SerializerTest.serializerTest
      [
        Busy (1, "test", 12345L)
        Empty
        Removed
      ]

  let hash x = x.GetHashCode() |> int64

  let root () =
    let serializer = HashTableElementSerializer<string, int>(FixedStringSerializer(), IntSerializer())
    in StreamArray(new MemoryStreamSource([||]), serializer) :> IResizeArray<_>

  let empty () =
    HashTable<string, int>(hash, root ())

  let ``Insert and TryFind Test`` =
    test {
      let map         = empty ()
      // The length increases after insertion.
      do! map.Length |> assertEquals 0L
      let ()          = map.Insert("test", 1, fun _ _ -> failwith "unexpected")
      do! map.Length |> assertEquals 1L
      // A value should be found iff it has been inserted.
      do! map.TryFind("test") |> assertEquals (Some 1)
      do! map.TryFind("????") |> assertEquals None
      // Inserted value and the value with the same key which already exists should be merged.
      let! e          = trap { it (map.Insert("test", 2, fun v v' -> failwithf "%d,%d" v v')) }
      do! e.Message |> assertEquals "1,2"
    }

  let xs =
    [ for i in 0..15 -> (sprintf "%04d" i, i) ]

  let serialNumbers () =
    let map = empty ()
    for (k, v) in xs do
      map.Insert(k, v, fun _ _ -> failwith "unexpected")
    map

  let insertManyTest =
    let map = serialNumbers ()
    let body (k, v) =
      test {
        // It should be true to insert values more than the initial capacity.
        do! map.Length |> assertEquals (xs |> List.length |> int64)
        do! map.TryFind(k) |> assertEquals (Some v)
      }
    parameterize {
      source xs
      run body
    }

  let updateTest =
    test {
      let map       = serialNumbers ()
      // Update should replace the value with the same key.
      let ()        = map.Update("0004", 42)
      do! map.TryFind("0004") |> assertEquals (Some 42)
    }

  let removeTest =
    test {
      let map       = serialNumbers ()
      let len       = map.Length
      // Remove should success iff the map contains some element with the key.
      do! map.Remove("0003") |> assertPred
      do! map.Remove("9999") |> not |> assertPred
      // The length decreases after removing.
      do! map.Length |> assertEquals (len - 1L)
      // Removed element shouldn't be found.
      do! map.TryFind("0003") |> assertEquals None
    }

  let ``Remove and TryFind Test`` =
    let map       = serialNumbers ()
    let _         = map.Remove("0009")
    let body (k, v) =
      test {
        // Any elements should be found even after removing.
        do! map.TryFind(k) |> assertEquals (if k = "0009" then None else Some v)
      }
    parameterize {
      source xs
      run body
    }

module MultiHashTableTest =
  open HashTableDetail
  open HashTableTest

  let empty () =  
    MultiHashTable<string, int>(hash, root ())

  let seed () =
    let mmap = empty ()
    let () =
      for (k, v) in xs do
        mmap.Insert(k, v)
    in mmap

  let insertTest =
    test {
      let mmap = seed ()
      return ()
    }

  let lengthTest =
    test {
      let mmap = seed ()
      do! mmap.Length |> assertEquals 16L
    }

  let findAllTest =
    test {
      let mmap = seed ()
      do! mmap.FindAll("9999") |> assertSatisfies Seq.isEmpty
      do! mmap.FindAll("0001") |> Seq.toList |> assertEquals [1]
    }

  let duplicatedKeyTest =
    test {
      let mmap = seed ()
      mmap.Insert("0001", -1)
      do! mmap.FindAll("0001") |> Seq.toList |> assertEquals [1; -1]
    }

  let removeAllTest =
    test {
      let mmap = seed ()
      mmap.Insert("0001", -1)
      do! mmap.RemoveAll("0001") |> Seq.toList |> assertEquals [1; -1]
      do! mmap.FindAll("0001") |> assertSatisfies Seq.isEmpty
    }
