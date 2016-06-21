namespace TokiwaDb.Core

open System
open Microsoft.FSharp.Reflection

type INongenericFixedLengthSerializer =
  abstract member Length: int64
  abstract member Serialize: obj -> array<byte>
  abstract member Deserialize: array<byte> -> obj

/// A serializer which can serialize a value to a byte array with fixed length;
/// and can deserialize the byte array to the original value.
type [<AbstractClass>] FixedLengthSerializer<'x>() =
  abstract member Serialize: 'x -> array<byte>
  abstract member Deserialize: array<byte> -> 'x
  abstract member Length: int64

  interface INongenericFixedLengthSerializer with
    override this.Length = this.Length

    override this.Serialize(value: obj) =
      this.Serialize(value :?> 'x)

    override this.Deserialize(data) =
      this.Deserialize(data) :> obj

module FixedLengthSerializerDetail =
  let serializeSequence (xs: array<obj>) (serializers: array<INongenericFixedLengthSerializer>) =
    Array.zip xs serializers
    |> Array.collect (fun (field, serializer) -> serializer.Serialize(field))

  let deserializeSequence
      (data: array<byte>) (i: int)
      (serializers: array<INongenericFixedLengthSerializer>)
    =
    let p = ref i
    in
      [|
        for serializer in serializers do
          let q = (! p) + (int serializer.Length)
          yield serializer.Deserialize(data.[(! p)..(q - 1)])
          p := q
      |]

open FixedLengthSerializerDetail

type FixedLengthArraySerializer<'x>
  ( _serializer: FixedLengthSerializer<'x>
  , _length: int64
  ) =
  inherit FixedLengthSerializer<array<'x>>()

  let _dataLength =
    _serializer.Length * _length

  override this.Length =
    _dataLength

  override this.Serialize(xs) =
    assert (xs.LongLength = _length)
    [|
      for x in xs do
        yield! _serializer.Serialize(x)
    |]

  override this.Deserialize(data) =
    assert (data.LongLength <= _dataLength)
    [|
      for i in 0L..(_length - 1L) do
        let p = i * _serializer.Length |> int
        let q = (i + 1L) * _serializer.Length - 1L |> int
        yield _serializer.Deserialize(data.[p..q])
    |]

type FixedLengthTupleSerializer<'t>(_serializers: array<INongenericFixedLengthSerializer>) =
  inherit FixedLengthSerializer<'t>()

  static do
    assert (FSharpType.IsTuple(typeof<'t>))

  do
    assert (FSharpType.GetTupleElements(typeof<'t>).Length = _serializers.Length)

  override this.Length =
    _serializers |> Array.sumBy (fun fls -> fls.Length)

  override this.Serialize(t: 't) =
    serializeSequence (FSharpValue.GetTupleFields(t)) _serializers

  override this.Deserialize(data) =
    let elements = deserializeSequence data 0 _serializers
    in FSharpValue.MakeTuple(elements, typeof<'t>) :?> 't

type FixedLengthDoubleSerializer<'x0, 'x1>
  ( _serializer0: FixedLengthSerializer<'x0>
  , _serializer1: FixedLengthSerializer<'x1>
  ) =
  inherit FixedLengthTupleSerializer<'x0 * 'x1>([| _serializer0; _serializer1 |])

type FixedLengthQuadrupleSerializer<'x0, 'x1, 'x2, 'x3>
  ( _serializer0: FixedLengthSerializer<'x0>
  , _serializer1: FixedLengthSerializer<'x1>
  , _serializer2: FixedLengthSerializer<'x2>
  , _serializer3: FixedLengthSerializer<'x3>
  ) =
  inherit FixedLengthTupleSerializer<'x0 * 'x1 * 'x2 * 'x3>([| _serializer0; _serializer1; _serializer2; _serializer3 |])

type FixedLengthUnionSerializer<'u>(_serializers: array<INongenericFixedLengthSerializer>) =
  inherit FixedLengthSerializer<'u>()

  let _cases = FSharpType.GetUnionCases(typeof<'u>)

  static do
    assert (FSharpType.IsUnion(typedefof<'u>))

  do
    assert (_cases.Length = _serializers.Length)
    assert (0 < _cases.Length && _cases.Length <= 0x100)

  let _length =
    (_serializers |> Array.map (fun s -> s.Length) |> Array.max) + 1L

  override this.Length =
    _length

  override this.Serialize(u: 'u) =
    let (case, values)  = FSharpValue.GetUnionFields(u, typeof<'u>)
    let tag             = case.Tag |> byte
    let serializer      = _serializers.[case.Tag]
    let fields          = case.GetFields()
    let data            =
      match fields.Length with
      | 0 -> [||]
      | 1 -> serializer.Serialize(values.[0])
      | _ ->
        let fieldTypes  = fields |> Array.map(fun pi -> pi.PropertyType)
        let tupleType   = FSharpType.MakeTupleType(fieldTypes)
        in serializer.Serialize(FSharpValue.MakeTuple(values, tupleType))
    let tailpad         =
      Array.zeroCreate (this.Length - (1L + data.LongLength) |> int)
    in
      [|
        yield tag
        yield! data
        yield! tailpad
      |]

  override this.Deserialize(data) =
    let tag         = data.[0] |> int
    let case        = _cases.[tag]
    let fields      = case.GetFields()
    let serializer  = _serializers.[tag]
    let data        = data.[1..]
    let values      =
      match fields.Length with
      | 0 -> [||]
      | 1 -> [| serializer.Deserialize(data) |]
      | _ -> FSharpValue.GetTupleFields(serializer.Deserialize(data))
    in FSharpValue.MakeUnion(case, values) |> unbox<'u>

type Int64Serializer() =
  inherit FixedLengthSerializer<int64>()

  override this.Length = 8L

  override this.Serialize(value: int64) =
    BitConverter.GetBytes(value)

  override this.Deserialize(data) =
    BitConverter.ToInt64(data, 0)

type FloatSerializer() =
  inherit FixedLengthSerializer<float>()

  override this.Length = 8L

  override this.Serialize(value: float) =
    BitConverter.GetBytes(value)

  override this.Deserialize(data) =
    BitConverter.ToDouble(data, 0)

type DateTimeSerializer() =
  inherit FixedLengthSerializer<DateTime>()

  override this.Length = 8L

  override this.Serialize(value: DateTime) =
    BitConverter.GetBytes(value.ToBinary())

  override this.Deserialize(data) =
    DateTime.FromBinary(BitConverter.ToInt64(data, 0))
