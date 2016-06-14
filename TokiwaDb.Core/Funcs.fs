namespace TokiwaDb.Core

open System
open System.IO
open FsYaml

module ValuePointer =
  let ofUntyped type' (p: int64) =
    match type' with
    | TInt      -> PInt p
    | TFloat    -> PFloat (BitConverter.Int64BitsToDouble p)
    | TString   -> PString p
    | TTime     -> PTime (DateTime.FromBinary(p))

  let toUntyped =
    function
    | PInt p    -> p
    | PFloat d  -> BitConverter.DoubleToInt64Bits(d)
    | PString p -> p
    | PTime t   -> t.ToBinary()

module Schema =
  let toFields (schema: Schema) =
    let keyFields =
      match schema.KeyFields with
      | Id -> [| Field ("id", TInt) |]
      | KeyFields keyFields -> keyFields
    in
      Array.append keyFields schema.NonkeyFields

  let readFromStream (stream: Stream) =
    stream |> Stream.readToEnd |> Yaml.load<Schema>

  let writeToStream (stream: Stream) schema =
    stream |> Stream.writeString (schema |> Yaml.dump<Schema>)

module Mortal =
  let maxLifeSpan =
    Int64.MaxValue

  let create t value =
    {
      Begin     = t
      End       = maxLifeSpan
      Value     = value
    }

  let isAliveAt t (mortal: Mortal<_>) =
    mortal.Begin <= t && t < mortal.End

  let kill t (mortal: Mortal<_>) =
    if mortal |> isAliveAt t
    then { mortal with End = t }
    else mortal

type RevisionServer(_id: RevisionId) =
  let mutable _id = _id

  new() =
    RevisionServer(0L)

  interface IRevisionServer with
    override this.Current =
      _id

    override this.Next() =
      _id <- _id + 1L
      _id
