namespace TokiwaDb.Core

open System.Threading

type MemoryTransaction(_performImpl: Operation -> unit, _revisionServer: RevisionServer) =
  inherit ImplTransaction()

  let _syncRoot = new obj()

  let _perform operation =
    lock _syncRoot (fun () ->
      _performImpl operation
      _revisionServer.Increase() |> ignore
      )

  let mutable _operationsStack = ([]: list<Operation>)

  let _add update =
    lock _syncRoot (fun () ->
      match _operationsStack with
      | [] ->
        Operation.empty |> update |> _perform
      | operation :: stack ->
        _operationsStack <- (operation |> update) :: stack
      )

  let _commit () =
    lock _syncRoot (fun () ->
      match _operationsStack with
      | [] ->
        failwith "Can't commit before beginning a transaction."
      | [operation] ->
        operation |> _perform
        _operationsStack <- []
      | r :: l :: stack ->
        _operationsStack <- Operation.merge l r :: stack
      )

  let _rollback () =
    lock _syncRoot (fun () ->
      match _operationsStack with
      | [] ->
        failwith "Can't rollback before beginning a transaction."
      | _ :: stack ->
        _operationsStack <- stack
      )

  override this.BeginCount =
    _operationsStack |> List.length

  override this.Operations =
    _operationsStack |> List.rev :> seq<Operation>

  override this.Begin() =
    _operationsStack <- Operation.empty :: _operationsStack

  override this.Add(operation) =
    _add operation

  override this.Commit() =
    _commit ()

  override this.Rollback() =
    _rollback ()

  override this.SyncRoot = _syncRoot

  override this.RevisionServer = _revisionServer
