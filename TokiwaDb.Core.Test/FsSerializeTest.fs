namespace TokiwaDb.Core.Test

open System
open System.IO
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.Core.FsSerialize

[<AutoOpen>]
module CustomPublic =
  open TokiwaDb.Core.FsSerialize.Public

  let customDefinitions =
    []

  let serializedLength<'x> () =
    serializedLengthWith<'x> customDefinitions

  module Stream =
    let serialize<'x> x stream =
      stream |> Stream.serializeWith<'x> customDefinitions x

    let deserialize<'x> stream =
      stream |> Stream.deserializeWith<'x> customDefinitions

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

  let assertLengthEquals expectedLength actualLength =
    test {
      match expectedLength with
      | Length.Flex ->
        do! assertPred true
      | Length.Fixed expectedLength ->
        do! actualLength |> assertEquals expectedLength
    }

  let testOnStream<'x> (stream: Stream) (x: 'x) =
    test {
      let expectedLength        = serializedLength<'x> ()
      let initialPosition       = stream.Position
      // Serialize test.
      let ()                    = stream |> Stream.serialize<'x> x
      let serializedLength      = stream.Position - initialPosition
      do! serializedLength |> assertLengthEquals expectedLength
      // Seek back to the initial position.
      let () = stream.Seek(-serializedLength, SeekOrigin.Current) |> ignore
      // Deserialize test.
      let x'                    = stream |> Stream.deserialize<'x>
      let deserializedLength    = stream.Position - initialPosition
      do! deserializedLength |> assertLengthEquals expectedLength
      return x'
    }

  let testSerialize<'x when 'x: equality> (x: 'x) =
    test {
      // Empty stream.
      let emptyStream   = new MemoryStream()
      let! x'           = testOnStream emptyStream x
      do! x' |> assertSatisfies ((=) x)
      // Random stream.
      let randomStream  = randomStream ()
      let! x'           = testOnStream randomStream x
      do! x' |> assertSatisfies ((=) x)
    }

  let intTest =
    parameterize {
      case 0
      case Int32.MaxValue
      case Int32.MinValue
      run testSerialize
    }

  let int64Test =
    parameterize {
      case 0L
      case Int64.MaxValue
      case Int64.MinValue
      run testSerialize
    }

  let floatTest =
    parameterize {
      case Math.PI
      case Double.MinValue
      case Double.PositiveInfinity
      run testSerialize
    }

  let nanTest =
    test {
      let stream        = new MemoryStream()
      let! x'           = testOnStream stream Double.NaN
      do! x' |> assertSatisfies Double.IsNaN
    }

  let stringTest =
    parameterize {
      case ""
      case "hello, world!"
      case "こんにちわ, 世界!"
      run testSerialize
    }

  let dateTimeTest =
    parameterize {
      case DateTime.Now
      case DateTime.MaxValue
      run testSerialize
    }

  let tuple2Test =
    parameterize {
      case (2L, Math.PI)
      case (-1L, Double.NegativeInfinity)
      run testSerialize
    }

  let tuple3Test =
    parameterize {
      case (3L, Math.E, DateTime.MinValue)
      run testSerialize
    }

  let arrayTest =
    parameterize {
      case [||]
      case [| 1L; 2L; 3L |]
      run testSerialize
    }

  module UnionTest =
    type FixedLengthUnion =
      | NoFieldCase
      | OneFieldCase    of int64
      | TwoFieldCase    of int64 * float

    type FlexLengthUnion =
      | StringFieldCase of string
      //| RecursiveCase   of FlexLengthUnion

    let serializedLengthTest =
      test {
        do! serializedLength<FixedLengthUnion> () |> assertEquals (Length.Fixed 17L)
        do! serializedLength<FlexLengthUnion> () |> assertEquals (Length.Flex)
      }

    let fixedLengthUnionTest =
      parameterize {
        case (NoFieldCase)
        case (OneFieldCase 1L)
        case (TwoFieldCase (1L, 2.0))
        run testSerialize
      }

    let flexLengthUnionTest =
      parameterize {
        case (StringFieldCase "hello")
        //case (RecursiveCase (StringFieldCase "world"))
        run testSerialize
      }

  module RecordTest =
    type FixedLengthRecord =
      {
        UnitField       : unit
        IntField        : int64
        IntFloatField   : int64 * float
      }

    type FlexLengthRecord =
      {
        StringField     : string
        //RecursiveField  : option<FlexLengthRecord>
      }

    let fixedLengthRecordTest =
      parameterize {
        case
          {
            UnitField           = ()
            IntField            = 1L
            IntFloatField       = (2L, 2.0)
          }
        run testSerialize
      }

    let flexLengthRecordTest =
      parameterize {
        case
          {
            StringField = "hello"
            //RecursiveField = Some { StringField = "world"; RecursiveField = None }
          }
        run testSerialize
      }
