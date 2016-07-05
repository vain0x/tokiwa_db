namespace TokiwaDb.Core

open System
open System.IO
open System.Threading
open FsYaml

module Mortal =
  let maxLifeSpan =
    Int64.MaxValue

  let create t value =
    {
      Birth     = t
      Death     = maxLifeSpan
      Value     = value
    }

  let isAliveAt t (mortal: IMortal) =
    mortal.Birth <= t && t < mortal.Death

module MortalValue =
  let valueIfAliveAt t (mortal: MortalValue<_>) =
    if mortal |> Mortal.isAliveAt t
    then mortal.Value |> Some
    else None

  let beBorn t (mortal: MortalValue<_>) =
    if mortal.Birth = Mortal.maxLifeSpan
    then { mortal with Birth = t }
    else mortal

  let kill t (mortal: MortalValue<_>) =
    if mortal |> Mortal.isAliveAt t
    then { mortal with Death = t }
    else mortal

  let map (f: 'x -> 'y) (m: MortalValue<'x>): MortalValue<'y> =
    {
      Birth     = m.Birth
      Death     = m.Death
      Value     = f m.Value
    }

  let readFromStream readValue (stream: Stream) =
    let birthRevision   = stream |> Stream.readInt64
    let deathRevision   = stream |> Stream.readInt64
    let value           = stream |> readValue
    in
      {
        Birth     = birthRevision
        Death     = deathRevision
        Value     = value
      }

  let writeToStream writeValue (stream: Stream) (this: MortalValue<_>) =
    stream |> Stream.writeInt64 this.Birth
    stream |> Stream.writeInt64 this.Death
    stream |> writeValue this.Value
    
  /// Set to `t` the end of lifespan of the mortal value written at the current position.
  /// This doesn't modify the position.
  let killInStream t (stream: Stream) =
    stream.Seek(8L, SeekOrigin.Current) |> ignore
    stream |> Stream.writeInt64 t
    stream.Seek(-16L, SeekOrigin.Current) |> ignore

module Value =
  let toType =
    function
    | Int _    -> TInt
    | Float _  -> TFloat
    | String _ -> TString
    | Binary _ -> TBinary
    | Time _   -> TTime

module ValuePointer =
  let ofUntyped type' (p: int64) =
    match type' with
    | TInt      -> PInt p
    | TFloat    -> PFloat (BitConverter.Int64BitsToDouble p)
    | TString   -> PString p
    | TBinary   -> PBinary p
    | TTime     -> PTime (DateTime.FromBinary(p))

  let toUntyped =
    function
    | PInt p    -> p
    | PFloat d  -> BitConverter.DoubleToInt64Bits(d)
    | PString p -> p
    | PBinary p -> p
    | PTime t   -> t.ToBinary()

  let serialize vp =
    BitConverter.GetBytes(vp |> toUntyped)

  let hash vp =
    vp |> toUntyped

  let serializer =
    FixedLengthUnionSerializer<ValuePointer>
      ([|
        Int64Serializer()
        FloatSerializer()
        Int64Serializer()
        Int64Serializer()
        DateTimeSerializer()
      |])

module Record =
  let toType record =
    record |> Array.map Value.toType

  let tryId record =
    match record |> Array.tryHead with
    | Some (Int recordId) -> recordId |> Some
    | _ -> None

  let dropId (record: Record) =
    record.[1..]

module RecordPointer =
  let hash recordPointer =
    recordPointer |> Array.map ValuePointer.hash |> Array.hash |> int64

  let serializer len =
    FixedLengthArraySerializer(ValuePointer.serializer, len)

  let tryId recordPointer =
    match recordPointer |> Array.tryHead with
    | Some (PInt recordId) -> recordId |> Some
    | _ -> None

  let dropId (recordPointer: RecordPointer) =
    recordPointer.[1..]

  let readFromStream fields (stream: Stream) =
    [|
      for Field (_, type') in fields do
        yield stream |> Stream.readInt64 |> ValuePointer.ofUntyped type'
    |]

  let writeToStream (stream: Stream) recordPointer =
    for valuePointer in recordPointer do
      stream |> Stream.writeInt64 (valuePointer |> ValuePointer.toUntyped)

module Field =
  let toType (Field (_, type')) =
    type'

  let int name =
    Field (name, TInt)

  let float name =
    Field (name, TFloat)

  let string name =
    Field (name, TString)

  let binary name =
    Field (name, TBinary)

  let time name =
    Field (name, TTime)

module TableSchema =
  let empty name =
    {
      Name              = name
      Fields            = [||]
      Indexes           = [||]
      LifeSpan          =
        {
          Birth         = Mortal.maxLifeSpan
          Death         = Mortal.maxLifeSpan
          Value         = ()
        }
    }

  let toFields (schema: TableSchema) =
    Array.append [| Field ("id", TInt) |] schema.Fields

type MemoryRevisionServer(_id: RevisionId) =
  inherit RevisionServer()
  let mutable _id = _id

  new() =
    MemoryRevisionServer(0L)

  override this.Current =
    _id

  override this.Next =
    _id + 1L

  override this.Increase() =
    Interlocked.Increment(& _id)
