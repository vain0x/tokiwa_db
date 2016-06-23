namespace TokiwaDb.Core

open System.Threading

type MemoryTransaction(_perform: array<Operation> -> unit) =
  inherit Transaction()

  let mutable _operationsStack = ([]: list<ResizeArray<Operation>>)

  override this.BeginCount =
    _operationsStack |> List.length

  override this.Operations =
    _operationsStack |> Seq.collect id

  override this.Begin() =
    _operationsStack <- ResizeArray<Operation>() :: _operationsStack

  override this.Add(operation) =
    match _operationsStack with
    | [] ->
      _perform [| operation |]
    | operations :: _ ->
      operations.Add(operation)

  override this.Commit() =
    match _operationsStack with
    | [] ->
      failwith "Can't commit before beginning a transaction."
    | [operations] ->
      operations.ToArray() |> _perform
      _operationsStack <- []
    | operations :: (operations' :: _ as stack) ->
      operations'.AddRange(operations)
      _operationsStack <- stack

  override this.Rollback() =
    match _operationsStack with
    | [] ->
      failwith "Can't rollback before beginning a transaction."
    | operations :: stack ->
      _operationsStack <- stack
