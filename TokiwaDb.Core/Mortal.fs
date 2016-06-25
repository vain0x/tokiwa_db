namespace TokiwaDb.Core

open System
open System.IO

type MortalValue<'x>(_value: 'x, _birth: RevisionId, _death: RevisionId) =
  inherit Mortal<'x>()

  let mutable _birth = _birth
  let mutable _death = _death

  new (value: 'x, birth: int64) =
    MortalValue(value, birth, Int64.MaxValue)

  new (value: 'x) =
    MortalValue(value, Int64.MaxValue)

  override this.Value = _value
  
  member this.Born = _birth < Int64.MaxValue
  member this.Died = _death < Int64.MaxValue

  member this.IsBorn(t) =
    assert (not this.Born && t < Int64.MaxValue)
    _birth <- t

  member this.Die(t) =
    assert (this.Born && not this.Died && t < Int64.MaxValue)
    _death <- t

  member this.Birth = _birth
  member this.Death = _death

  interface IMortal with
    override this.Birth = if _birth = Int64.MaxValue then Some _birth else None
    override this.Death = if _death = Int64.MaxValue then Some _death else None

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Mortal =
  let maxLifeSpan =
    Int64.MaxValue

  let create t value =
    MortalValue(value, t) :> IMortal

  let isAliveAt t (mortal: IMortal) =
    (mortal.Birth |> Option.getOr Int64.MaxValue) <= t
    && t < (mortal.Death |> Option.getOr Int64.MaxValue)

  let valueIfAliveAt t (mortal: Mortal<_>) =
    if mortal |> isAliveAt t
    then mortal.Value |> Some
    else None

  let map (f: 'x -> 'y) (m: MortalValue<'x>): MortalValue<'y> =
    MortalValue(f m.Value, m.Birth, m.Death)

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

  let writeToStream writeValue (stream: Stream) (this: Mortal<_>) =
    stream |> Stream.writeInt64 this.Birth
    stream |> Stream.writeInt64 this.Death
    stream |> writeValue this.Value
    
  /// Set to `t` the end of lifespan of the mortal value written at the current position.
  /// This doesn't modify the position.
  let killInStream t (stream: Stream) =
    stream.Seek(8L, SeekOrigin.Current) |> ignore
    stream |> Stream.writeInt64 t
    stream.Seek(-16L, SeekOrigin.Current) |> ignore
