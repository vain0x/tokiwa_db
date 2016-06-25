namespace TokiwaDb.Core

open System
open System.IO

module Mortal =
  let maxLifeSpan =
    Int64.MaxValue

  let create t value =
    {
      Birth     = t
      Death     = maxLifeSpan
      Value     = value
    }

  let isAliveAt t (mortal: Mortal<_>) =
    mortal.Birth <= t && t < mortal.Death

  let valueIfAliveAt t (mortal: Mortal<_>) =
    if mortal |> isAliveAt t
    then mortal.Value |> Some
    else None

  let kill t (mortal: Mortal<_>) =
    if mortal |> isAliveAt t
    then { mortal with Death = t }
    else mortal

  let map (f: 'x -> 'y) (m: Mortal<'x>): Mortal<'y> =
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
