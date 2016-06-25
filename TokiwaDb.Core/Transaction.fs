namespace TokiwaDb.Core

open System.Threading

type MemoryTransaction(_performImpl: array<Operation> -> unit, _revisionServer: RevisionServer) =
  inherit Transaction()

  let _syncRoot = new obj()

  let _perform operations =
    lock _syncRoot (fun () ->
      _performImpl operations
      _revisionServer.Increase() |> ignore
      )

  let mutable _operationsStack = ([]: list<ResizeArray<Operation>>)

  let _add operation =
    lock _syncRoot (fun () ->
      match _operationsStack with
      | [] ->
        _perform [| operation |]
      | operations :: _ ->
        operations.Add(operation)
      )

  let _commit () =
    lock _syncRoot (fun () ->
      match _operationsStack with
      | [] ->
        failwith "Can't commit before beginning a transaction."
      | [operations] ->
        operations.ToArray() |> _perform
        _operationsStack <- []
      | operations :: (operations' :: _ as stack) ->
        operations'.AddRange(operations)
        _operationsStack <- stack
      )

  let _rollback () =
    lock _syncRoot (fun () ->
      match _operationsStack with
      | [] ->
        failwith "Can't rollback before beginning a transaction."
      | operations :: stack ->
        _operationsStack <- stack
      )

  override this.BeginCount =
    _operationsStack |> List.length

  override this.Operations =
    _operationsStack |> Seq.collect id

  override this.Begin() =
    _operationsStack <- ResizeArray<Operation>() :: _operationsStack

  override this.Add(operation) =
    _add operation

  override this.Commit() =
    _commit ()

  override this.Rollback() =
    _rollback ()

  override this.SyncRoot = _syncRoot

  override this.RevisionServer = _revisionServer
