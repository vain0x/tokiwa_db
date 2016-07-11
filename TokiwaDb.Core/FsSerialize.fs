namespace TokiwaDb.Core.FsSerialize

// Reference: https://github.com/bleis-tift/FsYaml

open System
open System.IO
open System.Text
open Microsoft.FSharp.Reflection
open TokiwaDb.Core

type [<AbstractClass>] RuntimeBijective() =
  abstract member ConvertType: sourceType: Type -> Type
  abstract member Convert: sourceType: Type * sourceValue: obj -> obj
  abstract member Invert:  sourceType: Type * destValue: obj -> obj

type [<AbstractClass>] Bijective<'x, 'y>() =
  inherit RuntimeBijective()

  abstract member Convert: 'x -> 'y
  abstract member Invert: 'y -> 'x

  override this.ConvertType(_) = typeof<'y>

  override this.Convert(_, x: obj) =
    this.Convert(x :?> 'x) :> obj

  override this.Invert(_, y: obj) =
    this.Invert(y :?> 'y) :> obj

module DateTime =
  let bijectiveToInt64 =
    { new Bijective<DateTime, int64>() with
        override this.Convert(dateTime) = dateTime.ToBinary()
        override this.Invert(data)      = DateTime.FromBinary(data)
    }

module FSharpValue =
  module Record =
    let toTupleType recordType =
      let fields        = FSharpType.GetRecordFields(recordType)
      let types         = fields |> Array.map (fun pi -> pi.PropertyType)
      let tupleType     = FSharpType.MakeTupleType(types)
      in tupleType

    let toTuple recordType recordValue =
      let tupleType     = toTupleType recordType
      let values        = FSharpValue.GetRecordFields(recordValue)
      let tupleValue    = FSharpValue.MakeTuple(values, tupleType)
      in tupleValue

    let ofTuple recordType tupleValue =
      let fields        = FSharpType.GetRecordFields(recordType)
      let types         = fields |> Array.map (fun pi -> pi.PropertyType)
      let values        = FSharpValue.GetTupleFields(tupleValue)
      let recordValue   = FSharpValue.MakeRecord(recordType, values)
      in recordValue

    let bijectiveToTuple =
      { new RuntimeBijective() with
          override this.ConvertType(recordType) =
            toTupleType recordType
          override this.Convert(recordType, recordValue) =
            toTuple recordType recordValue
          override this.Invert(recordType, tupleValue) =
            ofTuple recordType tupleValue
      }

  module Function =
    let ofClosure sourceType rangeType f =
      let mappingFunctionType = typedefof<_ -> _>.MakeGenericType([| sourceType; rangeType |])
      in FSharpValue.MakeFunction(mappingFunctionType, fun x -> f x :> obj)

module ObjectElementSeq =
  open System
  open System.Linq
  open Microsoft.FSharp.Reflection

  let cast (t: Type) (xs: obj seq) =
    let enumerable = typeof<Enumerable>
    let cast =
      let nonGeneric = enumerable.GetMethod("Cast")
      nonGeneric.MakeGenericMethod([| t |])
    cast.Invoke(null, [| xs |])

  let toArray (t: Type) (xs: obj seq) =
    let array = Array.CreateInstance(t, xs.Count())
    xs |> Seq.iteri (fun i x -> array.SetValue(x, i))
    box array

module RuntimeSeq =
  open System
  open Microsoft.FSharp.Reflection
  
  let fsharpAsembly = typedefof<list<_>>.Assembly

  let seqModule = fsharpAsembly.GetType("Microsoft.FSharp.Collections.SeqModule")

  let elementType (t: Type) =
    if t.IsArray then
      t.GetElementType()
    else
      t.GetGenericArguments().[0]

  let invokeMethod name seqType typeArgs args =
    seqModule
      .GetMethod(name)
      .MakeGenericMethod(Array.append [| elementType seqType |] typeArgs)
      .Invoke(null, args)

  let length (seqType: Type) (xs: obj) =
    invokeMethod "Length" seqType [||] [| xs |] :?> int

  let iter (f: obj -> unit) (seqType: Type) (xs: obj) =
    let mapping =
      f |> FSharpValue.Function.ofClosure (elementType seqType) typeof<unit>
    in
      invokeMethod "Iterate" seqType [||] [| mapping; xs |] :?> unit

[<AutoOpen>]
module Types =
  [<RequireQualifiedAccess>]
  type Length =
    | Flex
    | Fixed of int64

  type LengthCalculator =
    Type -> Length

  type Serializer =
    Stream -> Type -> obj-> unit

  type Deserializer =
    Stream -> Type -> obj

  type RecursiveFunctions =
    {
      Length            : LengthCalculator
      Serialize         : Serializer
      Deserialize       : Deserializer
    }

  type Recursive<'f> =
    'f -> 'f

  type TypeDefinition =
    {
      Accept            : Type -> bool
      Length            : Recursive<LengthCalculator>
      Serialize         : LengthCalculator -> Recursive<Serializer>
      Deserialize       : LengthCalculator -> Recursive<Deserializer>
    }

module Length =
  let ofOption =
    function
    | None                -> Length.Flex
    | Some length         -> Length.Fixed length

  let toOption =
    function
    | Length.Flex         -> None
    | Length.Fixed length -> Some length

  let map f length =
    length
    |> toOption
    |> Option.map f
    |> ofOption

  let mapSeq f lengths =
    lengths
    |> Seq.map toOption
    |> Option.sequence
    |> Option.map f
    |> ofOption

  let sum lengths = mapSeq Seq.sum lengths
  let max lengths = mapSeq Seq.max lengths

  let toInt64 =
    toOption >> Option.get

module Primitives =
  let rec length definitions type' =
    match definitions |> Seq.tryFind (fun def -> def.Accept type') with
    | Some def ->
      let length'       = length definitions
      in def.Length length' type'
    | None -> failwithf "Can't calculate serialized length of %s." (string type')

  let rec serialize definitions stream type' value =
    match definitions |> Seq.tryFind (fun def -> def.Accept type') with
    | Some def ->
      let length'       = length definitions
      let serialize'    = serialize definitions
      in def.Serialize length' serialize' stream type' value
    | None -> failwithf "Can't serialize %A." value

  let rec deserialize definitions stream type' =
    match definitions |> Seq.tryFind (fun def -> def.Accept type') with
    | Some def ->
      let length'       = length definitions
      let deserialize'  = deserialize definitions
      in def.Deserialize length' deserialize' stream type'
    | None -> failwithf "Can't deserialize %s." (string type')

module TypeDefinitions =
  module Custom =
    type TypeDefinition = Types.TypeDefinition

    let serializeFixedLengthSeq _ serialize' stream seqType (value: obj) =
      let elementType   = seqType |> RuntimeSeq.elementType
      in value |> RuntimeSeq.iter (serialize' stream elementType) seqType

    let serializeSeq length' serialize' stream seqType (value: obj) =
      let length        = value |> RuntimeSeq.length seqType |> int64
      let ()            = stream |> Stream.writeInt64 length
      in serializeFixedLengthSeq length' serialize' stream seqType value

    let scalarTypeDefinition<'x> length serialize (deserialize: array<byte> -> 'x) =
      {
        Accept          = (=) typeof<'x>
        Serialize       = fun _ _ stream _ value ->
          stream |> Stream.writeBytes (value :?> 'x |> serialize)
        Deserialize     = fun _ _ stream _ ->
          (stream |> Stream.readBytes length |> deserialize) :> obj
        Length          = fun _ _ ->
          length |> int64 |> Length.Fixed
      }

    let ofBijective accept (bijective: RuntimeBijective) =
      {
        Accept        = accept
        Length        = fun length' typ ->
          let typ'    = bijective.ConvertType(typ)
          in length' typ'
        Serialize     = fun _ serialize' stream typ value ->
          let typ'    = bijective.ConvertType(typ)
          let value'  = bijective.Convert(typ, value)
          in serialize' stream typ' value'
        Deserialize   = fun _ deserialize' stream typ ->
          let typ'    = bijective.ConvertType(typ)
          let value'  = deserialize' stream typ'
          in bijective.Invert(typ, value')
      }

  open Custom

  let unitTypeDefinition =
    {
      Accept            = (=) typeof<unit>
      Length            = fun _ _ -> Length.Fixed 0L
      Serialize         = fun _ _ _ _ _ -> ()
      Deserialize       = fun _ _ _ _ -> () :> obj
    }

  let intTypeDefinition =
    scalarTypeDefinition<int>
      4
      (fun value -> BitConverter.GetBytes(value))
      (fun data -> BitConverter.ToInt32(data, 0))

  let int64TypeDefinition =
    scalarTypeDefinition<int64>
      8
      (fun value -> BitConverter.GetBytes(value))
      (fun data -> BitConverter.ToInt64(data, 0))

  let floatTypeDefinition =
    scalarTypeDefinition<float>
      8
      (fun value -> BitConverter.GetBytes(value))
      (fun data -> BitConverter.ToDouble(data, 0))

  let dateTimeTypeDefinition =
    ofBijective
      ((=) typeof<DateTime>)
      DateTime.bijectiveToInt64

  let stringTypeDefinition =
    {
      Accept            = (=) typeof<string>
      Length            = fun _ _ -> Length.Flex
      Serialize         = fun _ _ stream _ value ->
        let value       = value :?> string
        let data        = UTF8Encoding.UTF8.GetBytes(value)
        let _           = stream |> Stream.writeInt64 data.LongLength
        let ()          = stream |> Stream.writeBytes data
        in ()
      Deserialize       = fun _ _ stream _ ->
        let length      = stream |> Stream.readInt64 |> int
        let data        = stream |> Stream.readBytes length
        in UTF8Encoding.UTF8.GetString(data) :> obj
    }

  module Array =
    let accept (typ: Type) = typ.IsArray

    let fixedLengthDeserialize length length' deserialize' stream (arrayType: Type) =
      let elementType = arrayType.GetElementType()
      let values      = [| for i in 0L..(length - 1L) -> deserialize' stream elementType |]
      in ObjectElementSeq.toArray elementType values

    let definition =
      {
        Accept            = accept
        Length            = fun _ _ -> Length.Flex
        Serialize         = serializeSeq
        Deserialize       = fun length' deserialize' stream arrayType ->
          let length      = stream |> Stream.readInt64
          in fixedLengthDeserialize length length' deserialize' stream arrayType
      }

    let fixedLengthArrayTypeDefinition length =
      {
        Accept            = accept
        Length            = fun length' typ ->
          length' (typ.GetElementType()) |> Length.map ((*) length)
        Serialize         = serializeFixedLengthSeq
        Deserialize       = fixedLengthDeserialize length
      }

  module Tuple =
    let length (length': LengthCalculator) type' =
      FSharpType.GetTupleElements(type')
      |> Array.map length'
      |> Length.sum

    let serialize _ (serialize': Serializer) stream tupleType value =
      let types         = FSharpType.GetTupleElements(tupleType)
      let values        = FSharpValue.GetTupleFields(value)
      for (type', value) in Seq.zip types values do
        serialize' stream type' value

    let deserialize _ (deserialize': Deserializer) stream tupleType =
      let values =
        FSharpType.GetTupleElements(tupleType)
        |> Seq.map (fun type' -> deserialize' stream type')
        |> Seq.toArray
      in
        FSharpValue.MakeTuple(values, tupleType)

    let definition =
      {
        Accept          = fun type' -> FSharpType.IsTuple(type')
        Length          = length
        Serialize       = serialize
        Deserialize     = deserialize
      }

  module Union =
    let length (length': LengthCalculator) unionType =
      let lengthCase (case: UnionCaseInfo) =
        seq {
          yield Length.Fixed 1L // for tag
          for pi in case.GetFields() do
            yield length' pi.PropertyType
        } |> Length.sum
      in
        FSharpType.GetUnionCases(unionType)
        |> Array.map lengthCase
        |> Length.max

    let serializeBody serialize' stream (types: array<Type>) (values: array<obj>) =
      match types.Length with
      | 0 -> ()
      | 1 ->
        let type'               = types.[0]
        let value               = values.[0]
        in serialize' stream type' value
      | _ ->
        let tupleType           = FSharpType.MakeTupleType(types)
        let tupleValue          = FSharpValue.MakeTuple(values, tupleType)
        in serialize' stream tupleType tupleValue

    let serializeTailpad (stream: Stream) initialPosition totalLength =
      match totalLength with
      | Length.Flex -> ()
      | Length.Fixed totalLength ->
        let endPosition         = initialPosition + totalLength
        let footerLength        = endPosition - stream.Position
        stream |> Stream.writeBytes (footerLength |> int |> Array.zeroCreate)

    let serialize length' (serialize': Serializer) (stream: Stream) unionType value =
      let initialPosition       = stream.Position
      let (case, values)        = FSharpValue.GetUnionFields(value, unionType)
      let types                 = case.GetFields() |> Array.map (fun pi -> pi.PropertyType)
      let tag                   = case.Tag |> byte
      stream.WriteByte(tag)
      serializeBody serialize' stream types values
      serializeTailpad stream initialPosition (length' unionType)

    let deserializeBody deserialize' stream (types: array<Type>) =
      match types.Length with
      | 0 -> [||]
      | 1 -> [| deserialize' stream types.[0] |]
      | _ ->
        let tupleType           = FSharpType.MakeTupleType(types)
        let tupleValue          = deserialize' stream tupleType
        in FSharpValue.GetTupleFields(tupleValue)

    let skipFooter (stream: Stream) initialPosition totalLength =
      match totalLength with
      | Length.Flex -> ()
      | Length.Fixed totalLength ->
        stream.Seek(initialPosition + totalLength, SeekOrigin.Begin) |> ignore

    let deserialize length' (deserialize': Deserializer) (stream: Stream) unionType =
      let initialPosition       = stream.Position
      let tag                   = stream.ReadByte()
      let case                  = (FSharpType.GetUnionCases(unionType)).[tag]
      let types                 = case.GetFields() |> Array.map (fun pi -> pi.PropertyType)
      let values                = deserializeBody deserialize' stream types
      let value                 = FSharpValue.MakeUnion(case, values)
      skipFooter stream initialPosition (length' unionType)
      value

    let definition =
      {
        Accept          = fun type' -> FSharpType.IsUnion(type')
        Length          = length
        Serialize       = serialize
        Deserialize     = deserialize
      }

  let recordTypeDefinition =
    ofBijective
      (fun type' -> FSharpType.IsRecord(type'))
      FSharpValue.Record.bijectiveToTuple

  let primitiveDefinitions =
    [
      unitTypeDefinition
      intTypeDefinition
      int64TypeDefinition
      floatTypeDefinition
      dateTimeTypeDefinition
      stringTypeDefinition
      Array.definition
      Tuple.definition
      Union.definition
      recordTypeDefinition
    ]

module Public =
  open TypeDefinitions

  let serializedLengthWith<'x> customDefinitions =
    let definitions = List.append customDefinitions primitiveDefinitions
    in Primitives.length definitions typeof<'x>

  let serializedLength<'x> () =
    serializedLengthWith<'x> []

  module Stream =
    open TypeDefinitions

    let serializeWith<'x> customDefinitions =
      let definitions = List.append customDefinitions primitiveDefinitions
      fun (x: 'x) stream ->
        Primitives.serialize definitions stream typeof<'x> x

    let serialize<'x> x =
      serializeWith<'x> [] x

    let deserializeWith<'x> customDefinitions =
      let definitions = List.append customDefinitions primitiveDefinitions
      fun stream ->
        (Primitives.deserialize definitions stream typeof<'x>) :?> 'x

    let deserialize<'x> stream =
      deserializeWith<'x> [] stream

  module ByteArray =
    let serialize<'x> (x: 'x): array<byte> =
      use memoryStream = new MemoryStream()
      memoryStream |> Stream.serialize<'x> x
      memoryStream.ToArray()

    let deserialize<'x> (data: array<byte>): 'x =
      use memoryStream = new MemoryStream()
      memoryStream |> Stream.writeBytes data
      memoryStream |> Stream.deserialize<'x>
