namespace TokiwaDb.Core.Test

open System
open System.Text
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

type FixedStringSerializer() =
  inherit FixedLengthSerializer<string>()

  override this.Length = 4L

  override this.Serialize(s) =
    UTF8Encoding.UTF8.GetBytes(s)

  override this.Deserialize(bs) = UTF8Encoding.UTF8.GetString(bs)

type IntSerializer() =
  inherit FixedLengthSerializer<int>()

  override this.Length = 4L
  override this.Serialize(value) = BitConverter.GetBytes(value)
  override this.Deserialize(data) = BitConverter.ToInt32(data, 0)

module SerializerTest =
  let serializerTest (xs: seq<'x>) (serializer: FixedLengthSerializer<'x>) =
    let body (x: 'x) =
      test {
        let data = serializer.Serialize(x)
        do! data.LongLength |> assertEquals serializer.Length
        do! serializer.Deserialize(data) |> assertEquals x
      }
    parameterize {
      source xs
      run body
    }

  let int64SerializerTest =
    Int64Serializer() |> serializerTest [1L; Int64.MinValue]

  let floatSerializerTest =
    FloatSerializer() |> serializerTest [3.14; Double.MaxValue; Double.NegativeInfinity]

  let dateTimeSerializerTest =
    DateTimeSerializer() |> serializerTest [DateTime.Now]

  let fixedStringSerializerTest =
    FixedStringSerializer() |> serializerTest ["test"]

  let intSerializerTest =
    IntSerializer() |> serializerTest [0; 100; -1]

  let arraySerializerTest =
    FixedLengthArraySerializer(IntSerializer(), 3L)
    |> serializerTest [[|0; 1; 2|]]

  let doubleSerializerTest =
    FixedLengthDoubleSerializer(IntSerializer(), FixedStringSerializer())
    |> serializerTest [(3, "test")]

  let quadrupleSerializerTest =
    FixedLengthQuadrupleSerializer
      ( IntSerializer()
      , FixedStringSerializer()
      , IntSerializer()
      , FixedStringSerializer()
      )
    |> serializerTest [(1, "memo", 2, "note")]

  type TestUnion =
    | Unit
    | Int of int
    | Pair of string * int

  let unionSerializerTest =
    FixedLengthUnionSerializer<TestUnion>
      ([|
        IntSerializer() // Any serializer.
        IntSerializer()
        FixedLengthDoubleSerializer(FixedStringSerializer(), IntSerializer())
      |])
    |> serializerTest
      [
        Unit
        Int 1
        Pair("hoge", -1)
      ]
