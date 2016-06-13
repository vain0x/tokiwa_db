namespace TokiwaDb.Core

module T2 =
  let map f (x0, x1) = (f x0, f x1)

module Array =
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

module Seq =
  let equalAll xs =
    if xs |> Seq.isEmpty
    then true
    else xs |> Seq.skip 1 |> Seq.forall ((=) (xs |> Seq.head))
