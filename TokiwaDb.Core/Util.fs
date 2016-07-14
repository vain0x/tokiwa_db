namespace TokiwaDb.Core

[<AutoOpen>]
module Misc =
  let fold xs f s =
    xs |> Seq.fold (fun s x -> f x s) s

module Hash =
  /// Cloned from Boost hash_combine.
  let combine seed hash =
    seed ^^^ hash + 0x9e3779b9L + (seed <<< 6) + (seed >>> 2)

module Option =
  let getOr (x: 'x): option<'x> -> 'x =
    function
    | Some x -> x
    | None -> x

  let getOrElse (f: unit -> 'x): option<'x> -> 'x =
    function
    | Some x -> x
    | None -> f ()

  let ofPair =
    function
    | (true, x) -> Some x
    | (false, _) -> None

  let sequence os =
    os |> Seq.fold (fun state opt ->
      match (state, opt) with
      | (Some xs, Some x) -> (Some (x :: xs))
      | _ -> None
      ) (Some [])
    |> Option.map List.rev

module T2 =
  let map f (x0, x1) = (f x0, f x1)

module Array =
  let inline hash< ^x when ^x: (static member op_Explicit: ^x -> int64)> (xs: array< ^x>) =
    xs |> Array.fold (fun seed x -> Hash.combine seed (int64 x)) 0L

  let choosei f xs =
    xs |> Array.mapi f |> Array.choose id

  let indexed xs =
    xs |> Array.mapi (fun i x -> (i, x))

  let partitionMap (f: 'x -> option<'y>) (xs: array<'x>): (array<'y> * array<'x>) =
    [|
      for x in xs do
        match f x with
        | Some y    -> yield (Some y, None)
        | None      -> yield (None, Some x)
    |]
    |> Array.unzip
    |> (fun (ys, xs) -> (ys |> Array.choose id, xs |> Array.choose id))

  let tryHead xs =
    if xs |> Array.isEmpty
    then None
    else Some xs.[0]

module ResizeArray =
  let ofSeq (xs: seq<'x>): ResizeArray<'x> =
    ResizeArray(xs)

  let tryItem i (xs: ResizeArray<'x>) =
    if 0 <= i && i < xs.Count
    then xs.[i] |> Some
    else None

module IDictionary =
  open System.Collections.Generic

  let toPairSeq (dict: IDictionary<'k, 'v>): seq<'k * 'v> =
    dict |> Seq.map (fun (KeyValue kv) -> kv)

  let tryFind (key: 'k) (dict: IDictionary<'k, 'v>): option<'v> =
    match dict.TryGetValue(key) with
    | (true, value)     -> Some value
    | (false, _)        -> None

module Dictionary =
  open System.Collections.Generic

  let ofSeq (kvs: seq<'k * 'v>): Dictionary<'k, 'v> =
    let dict = Dictionary<'k, 'v>()
    for (k, v) in kvs do
      dict.Add(k, v)
    dict

module Map =
  let length (map: Map<_, _>) =
    map |> Seq.length

  let findOrInsert (key: 'k) (f: unit -> 'v) (map: Map<'k, 'v>) =
    match map |> Map.tryFind key with
    | Some value ->
      (map, value)
    | None ->
      let value = f ()
      in (map |> Map.add key value, value)

module Seq =
  let tryHead (xs: seq<'x>): option<'x> =
    if xs |> Seq.isEmpty
    then None
    else xs |> Seq.head |> Some

  let equalAll xs =
    if xs |> Seq.isEmpty
    then true
    else xs |> Seq.skip 1 |> Seq.forall ((=) (xs |> Seq.head))

module Type =
  open System

  let isGenericTypeDefOf genericTypeDef (typ: Type) =
    typ.IsGenericType
    && typ.GetGenericTypeDefinition() = genericTypeDef

[<AutoOpen>]
module DisposableExtensions =
  open System

  type RelayDisposable(_dispose: unit -> unit) =
    let mutable _isDisposed = false

    interface IDisposable with
      override this.Dispose() =
        if not _isDisposed then
          _isDisposed <- true
          _dispose ()

    override this.Finalize() =
      (this :> IDisposable).Dispose()

module FSharpValue =
  open Microsoft.FSharp.Reflection
  
  module Function =
    let ofClosure sourceType rangeType f =
      let mappingFunctionType = typedefof<_ -> _>.MakeGenericType([| sourceType; rangeType |])
      in FSharpValue.MakeFunction(mappingFunctionType, fun x -> f x :> obj)

module Stream =
  open System
  open System.IO
  open System.Text

  let readToEnd (stream: Stream) =
    let buffer    = Array.zeroCreate (int stream.Length)
    let _         = stream.Read(buffer, 0, buffer.Length)
    in UTF8Encoding.UTF8.GetString(buffer)

  let readBytes count (stream: Stream) =
    let buffer    = Array.zeroCreate count
    let _         = stream.Read(buffer, 0, count)
    in buffer

  let writeBytes data (stream: Stream) =
    stream.Write(data, 0, data.Length)

  let writeString (s:string) (stream: Stream) =
    let buffer    = UTF8Encoding.UTF8.GetBytes(s)
    in stream.Write(buffer, 0, buffer.Length)

  let readInt64 (stream: Stream) =
    BitConverter.ToInt64(stream |> readBytes 8, 0)

  let writeInt64 (n: int64) (stream: Stream) =
    stream |> writeBytes (BitConverter.GetBytes(n))

module FileInfo =
  open System.IO

  let exists (file: FileInfo) =
    File.Exists(file.FullName)

  let length (file: FileInfo) =
    let () =
      file.Refresh()
    in
      if file.Exists
      then file.Length
      else 0L

  let createNew (file: FileInfo) =
    use stream = file.Create() in ()

  let readText (file: FileInfo) =
    use streamReader = file.OpenText()
    in streamReader.ReadToEnd()
