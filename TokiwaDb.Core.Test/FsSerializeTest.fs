namespace TokiwaDb.Core.Test

open System
open System.IO
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.Core.FsSerialize

module FsSerializeTest =
  let randomStream =
    let rng             = Random()
    fun () ->
      let randomStream  = new MemoryStream()
      let buffer        = Array.zeroCreate 100
      rng.NextBytes(buffer)
      let position      = rng.Next(0, 100) |> int64
      randomStream.Seek(position, SeekOrigin.Begin) |> ignore
      randomStream

  let fixedLengthSerializeAndDeserializeTest<'x> (stream: Stream) (x: 'x) =
    test {
      let initialPosition       = stream.Position
      // Serialize test.
      let serializedLength      = stream |> Stream.serialize<'x> x
      let expectedLength        = Stream.serializedLength<'x> ()
      do! serializedLength |> assertEquals expectedLength
      do! (stream.Position - initialPosition) |> assertEquals expectedLength
      // Seek back to the initial position.
      stream.Seek(-serializedLength, SeekOrigin.Current) |> ignore
      // Deserialize test.
      let x'                    = stream |> Stream.deserialize<'x>
      do! (stream.Position - initialPosition) |> assertEquals expectedLength
      return x'
    }

  let fixedLengthTest<'x when 'x: equality> (x: 'x) =
    test {
      // Empty stream.
      use emptyStream   = new MemoryStream()
      let! x'           = fixedLengthSerializeAndDeserializeTest emptyStream x
      do! x' |> assertSatisfies ((=) x)
      // Random stream.
      use randomStream  = randomStream ()
      let! x'           = fixedLengthSerializeAndDeserializeTest randomStream x
      do! x' |> assertSatisfies ((=) x)
    }

  let flexLengthSerializeAndDeserializeTest<'x> (stream: Stream) (x: 'x) =
    test {
      // Serialize test.
      let beginPosition         = stream.Position
      let serializedLength      = stream |> Stream.serialize<'x> x
      let endPosition           = stream.Position
      do! serializedLength |> assertEquals (endPosition - beginPosition)
      // Deserialize test.
      stream.Seek(beginPosition, SeekOrigin.Begin) |> ignore
      let x'                    = stream |> Stream.deserialize<'x>
      do! stream.Position |> assertEquals endPosition
      return x'
    }

  let flexLengthTest<'x when 'x: equality> (x: 'x) =
    test {
      // Empty stream.
      use emptyStream   = new MemoryStream()
      let! x'           = flexLengthSerializeAndDeserializeTest<'x> emptyStream x
      do! x' |> assertSatisfies ((=) x)
    }

  let intTest =
    parameterize {
      case 0
      case Int32.MaxValue
      case Int32.MinValue
      run fixedLengthTest
    }

  let int64Test =
    parameterize {
      case 0L
      case Int64.MaxValue
      case Int64.MinValue
      run fixedLengthTest
    }

  let floatTest =
    parameterize {
      case Math.PI
      case Double.MinValue
      case Double.PositiveInfinity
      run fixedLengthTest
    }

  let nanTest =
    test {
      let stream        = new MemoryStream()
      let! x'           = fixedLengthSerializeAndDeserializeTest stream Double.NaN
      do! x' |> assertSatisfies Double.IsNaN
    }

  let dateTimeTest =
    parameterize {
      case DateTime.Now
      case DateTime.MaxValue
      run fixedLengthTest
    }

  let tuple2Test =
    parameterize {
      case (2L, Math.PI)
      case (-1L, Double.NegativeInfinity)
      run fixedLengthTest
    }

  let tuple3Test =
    parameterize {
      case (3L, Math.E, DateTime.MinValue)
      run fixedLengthTest
    }

  type TestingUnion =
    | NoFieldCase
    | OneFieldCase      of int64
    | TwoFieldCase      of int64 * float

  let unionTest =
    parameterize {
      case NoFieldCase
      case (OneFieldCase 1L)
      case (TwoFieldCase (1L, 2.0))
      run fixedLengthTest
    }

  type TestingRecord =
    {
      Field0            : unit
      Field1            : int64
      Field2            : int64 * float
    }

  let recordTest =
    parameterize {
      case { Field0 = (); Field1 = 1L; Field2 = (2L, 2.0) }
      run fixedLengthTest
    }

  let stringTest =
    parameterize {
      case ""
      case "hello, world!"
      case "こんにちわ, 世界!"
      run flexLengthTest
    }

  let arrayTest =
    parameterize {
      case [||]
      case [| 1L; 2L; 3L |]
      run flexLengthTest
    }
