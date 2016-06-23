namespace TokiwaDb.Core

open System.Threading

type MemoryTransaction(_perform: array<Operation> -> unit) =
  inherit Transaction()

  let mutable _beginCount = 1
  let mutable _operations = ResizeArray<Operation>()

  override this.BeginCount =
    _beginCount

  override this.Operations =
    _operations :> seq<Operation>

  override this.Rebegin() =
    Interlocked.Increment(& _beginCount) |> ignore

  override this.Add(operation) =
    _operations.Add(operation)

  override this.Commit() =
    if Interlocked.Decrement(& _beginCount) = 0 then
      _operations.ToArray() |> _perform

  override this.Rollback() =
    if Interlocked.Decrement(& _beginCount) = 0 then
      _operations.Clear()
