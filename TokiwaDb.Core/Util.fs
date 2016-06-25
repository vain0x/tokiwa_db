namespace TokiwaDb.Core

module Hash =
  /// Cloned from Boost hash_combine.
  let combine seed hash =
    seed ^^^ hash + 0x9e3779b9L + (seed <<< 6) + (seed >>> 2)

module Option =
  let getOr (x: 'x): option<'x> -> 'x =
    function
    | Some x -> x
    | None -> x

  let ofPair =
    function
    | (true, x) -> Some x
    | (false, _) -> None

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

module Map =
  let length (map: Map<_, _>) =
    map |> Seq.length

module Seq =
  let equalAll xs =
    if xs |> Seq.isEmpty
    then true
    else xs |> Seq.skip 1 |> Seq.forall ((=) (xs |> Seq.head))

module Int64 =
  let toByteArray n =
    Seq.unfold (fun (n, i) ->
      if i = 8
      then None
      else Some (byte (n &&& 0xFFL), (n >>> 8, i + 1))
      ) (n, 0)
    |> Seq.toArray
    |> Array.rev

  let ofByteArray (bs: array<byte>) =
    assert (bs.Length = 8)
    bs |> Array.fold (fun n b -> (n <<< 8) ||| (int64 b)) 0L

module Stream =
  open System.IO
  open System.Text

  let readToEnd (stream: Stream) =
    let buffer    = Array.zeroCreate (int stream.Length)
    let _         = stream.Read(buffer, 0, buffer.Length)
    in UTF8Encoding.UTF8.GetString(buffer)

  let writeString (s:string) (stream: Stream) =
    let buffer    = UTF8Encoding.UTF8.GetBytes(s)
    in stream.Write(buffer, 0, buffer.Length)

  let readInt64 (stream: Stream) =
    let bytes = Array.zeroCreate 8
    let _ = stream.Read(bytes, 0, bytes.Length)
    in bytes |> Int64.ofByteArray

  let writeInt64 (n: int64) (stream: Stream) =
    let bytes = n |> Int64.toByteArray
    in stream.Write(bytes, 0, bytes.Length)

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
