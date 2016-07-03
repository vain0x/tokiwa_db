namespace TokiwaDb.Core.FsSerialize

// Reference: https://github.com/bleis-tift/FsYaml

open System
open System.IO
open System.Text
open Microsoft.FSharp.Reflection
open TokiwaDb.Core

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

  let length (t: Type) (xs: obj): int =
    let elementType = elementType t
    let lengthFunc = seqModule.GetMethod("Length").MakeGenericMethod(elementType)
    lengthFunc.Invoke(null, [| xs |]) :?> int

  let map (f: obj -> 'a) (t: Type) (xs: obj): 'a seq =
    let elementType = elementType t
    let mapping =
      let mappingFunctionType = typedefof<_ -> _>.MakeGenericType([| elementType; typeof<'a> |])
      FSharpValue.MakeFunction(mappingFunctionType, fun x -> f x :> obj)
    let mapFunc = seqModule.GetMethod("Map").MakeGenericMethod(elementType, typeof<'a>)
    mapFunc.Invoke(null, [| mapping; xs |]) :?> seq<'a>

[<AutoOpen>]
module Types =
  type LengthCalculator =
    Type -> int64

  type Serializer =
    Stream -> Type -> obj-> int64

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
    let serializeSeq _ serialize' stream seqType (value: obj) =
      let elementType     = seqType |> RuntimeSeq.elementType
      let length          = value |> RuntimeSeq.length seqType |> int64
      let _               = stream |> Stream.writeInt64 length
      in
        value
        |> RuntimeSeq.map (serialize' stream elementType) seqType
        |> Seq.sum
        |> (+) 8L // the size of length value

    let scalarTypeDefinition<'x> length serialize (deserialize: array<byte> -> 'x) =
      {
        Accept            = (=) typeof<'x>
        Serialize         = fun _ _ stream _ value ->
          let () = stream |> Stream.writeBytes (value :?> 'x |> serialize)
          in length |> int64
        Deserialize       = fun _ _ stream _ ->
          (stream |> Stream.readBytes length |> deserialize) :> obj
        Length            = fun _ _ ->
          length |> int64
      }

  open Custom

  let unitTypeDefinition =
    {
      Accept            = (=) typeof<unit>
      Length            = fun _ _ -> 0L
      Serialize         = fun _ _ _ _ _ -> 0L
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
    scalarTypeDefinition<DateTime>
      8
      (fun value -> BitConverter.GetBytes(value.ToBinary()))
      (fun data -> DateTime.FromBinary(BitConverter.ToInt64(data, 0)))

  let stringTypeDefinition =
    {
      Accept            = (=) typeof<string>
      Length            = fun _ _ -> failwith "Not fixed length: string."
      Serialize         = fun _ _ stream _ value ->
        let value       = value :?> string
        let data        = UTF8Encoding.UTF8.GetBytes(value)
        let _           = stream |> Stream.writeInt64 data.LongLength
        let ()          = stream |> Stream.writeBytes data
        in 8L + data.LongLength
      Deserialize       = fun _ _ stream _ ->
        let length      = stream |> Stream.readInt64 |> int
        let data        = stream |> Stream.readBytes length
        in UTF8Encoding.UTF8.GetString(data) :> obj
    }

  module Tuple =
    let length (length': Type -> int64) type' =
      FSharpType.GetTupleElements(type')
      |> Array.sumBy length'

    let serialize _ (serialize': Serializer) stream tupleType value =
      let types         = FSharpType.GetTupleElements(tupleType)
      let values        = FSharpValue.GetTupleFields(value)
      in
        Seq.zip types values
        |> Seq.sumBy (fun (type', value) -> serialize' stream type' value)

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
        case.GetFields()
        |> Array.sumBy (fun pi -> length' pi.PropertyType)
        |> (+) 1L // for tag
      in
        FSharpType.GetUnionCases(unionType)
        |> Array.map lengthCase
        |> Array.max

    let serialize length' (serialize': Serializer) (stream: Stream) unionType value =
      let (case, values)        = FSharpValue.GetUnionFields(value, unionType)
      let types                 = case.GetFields() |> Array.map (fun pi -> pi.PropertyType)
      let tag                   = case.Tag |> byte
      // Header
      let headerLength          = 1L
      stream.WriteByte(tag)
      // Body
      let bodyLength =
        match types.Length with
        | 0 -> 0L
        | 1 ->
          let type'             = types.[0]
          let value             = values.[0]
          in serialize' stream type' value
        | _ ->
          let tupleType         = FSharpType.MakeTupleType(types)
          let tupleValue        = FSharpValue.MakeTuple(values, tupleType)
          in serialize' stream tupleType tupleValue
      // Tailpad
      let totalLength           = length' unionType
      let footerLength          = totalLength - (headerLength + bodyLength)
      stream |> Stream.writeBytes (footerLength |> int |> Array.zeroCreate)
      totalLength

    let deserialize length' (deserialize': Deserializer) (stream: Stream) unionType =
      let initialPosition       = stream.Position
      let headerLength          = 1L
      let tag                   = stream.ReadByte()
      let case                  = (FSharpType.GetUnionCases(unionType)).[tag]
      let types                 = case.GetFields() |> Array.map (fun pi -> pi.PropertyType)
      let values                =
        match types.Length with
        | 0 -> [||]
        | 1 -> [| deserialize' stream types.[0] |]
        | _ ->
          let tupleType         = FSharpType.MakeTupleType(types)
          let tupleValue        = deserialize' stream tupleType
          in FSharpValue.GetTupleFields(tupleValue)
      let resultValue           =
        FSharpValue.MakeUnion(case, values)
      let totalLength           = length' unionType
      stream.Seek(initialPosition + totalLength, SeekOrigin.Begin) |> ignore
      resultValue

    let definition =
      {
        Accept          = fun type' -> FSharpType.IsUnion(type')
        Length          = length
        Serialize       = serialize
        Deserialize     = deserialize
      }

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

    let length (length': LengthCalculator) recordType =
      FSharpType.GetRecordFields(recordType)
      |> Array.sumBy (fun pi -> length' pi.PropertyType)

    let serialize _ serialize' stream recordType value =
      let tupleType     = toTupleType recordType
      let tupleValue    = toTuple recordType value
      in serialize' stream tupleType tupleValue

    let deserialize _ deserialize' stream recordType =
      let tupleType     = toTupleType recordType
      let tupleValue    = deserialize' stream tupleType
      in ofTuple recordType tupleValue

    let definition =
      {
        Accept          = fun type' -> FSharpType.IsRecord(type')
        Length          = length
        Serialize       = serialize
        Deserialize     = deserialize
      }

  let arrayTypeDefinition =
    {
      Accept            = fun type' -> type'.IsArray
      Length            = fun _ _ -> failwith "Not fixed length: array<_>."
      Serialize         = serializeSeq
      Deserialize       = fun _ deserialize' stream arrayType ->
        let elementType = arrayType.GetElementType()
        let length      = stream |> Stream.readInt64
        let values      = [| for i in 0L..(length - 1L) -> deserialize' stream elementType |]
        in ObjectElementSeq.toArray elementType values
    }

  let primitiveDefinitions =
    [
      unitTypeDefinition
      intTypeDefinition
      int64TypeDefinition
      floatTypeDefinition
      dateTimeTypeDefinition
      stringTypeDefinition
      Tuple.definition
      Union.definition
      Record.definition
      arrayTypeDefinition
    ]

module Stream =
  open TypeDefinitions

  let serializedLength<'x> () =
    Primitives.length primitiveDefinitions typeof<'x>

  let serialize<'x> (x: 'x) stream =
    Primitives.serialize primitiveDefinitions stream typeof<'x> x

  let deserialize<'x> stream =
    (Primitives.deserialize primitiveDefinitions stream typeof<'x>) :?> 'x
