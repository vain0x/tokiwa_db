namespace TokiwaDb.Core

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
    assert (data.LongLength = _dataLength)
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
