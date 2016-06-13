namespace TokiwaDb.Core

module Array =
  let choosei f xs =
    xs |> Array.mapi f |> Array.choose id

module Seq =
  let equalAll xs =
    if xs |> Seq.isEmpty
    then true
    else xs |> Seq.skip 1 |> Seq.forall ((=) (xs |> Seq.head))
