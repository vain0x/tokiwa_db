namespace TokiwaDb.CodeFirst.Detail

open System
open Chessie.ErrorHandling
open TokiwaDb.Core
open TokiwaDb.CodeFirst

type OrmTable<'m when 'm :> IModel>(_impl: ImplTable) =
  inherit Table<'m>()

  let _modelType = typeof<'m>

  let _toModel (rp: MortalValue<RecordPointer>) =
    rp |> Model.ofMortalRecordPointer _modelType _impl.Database.Storage.Derefer
    :?> 'm

  let _item recordId =
    match _impl.RecordById(recordId) with
    | Some recordPointer ->
      recordPointer |> _toModel
    | None ->
      ArgumentOutOfRangeException() |> raise

  let _allItems () =
    _impl.RecordPointers
    |> Seq.map _toModel

  let _items () =
    _allItems ()
    |> Seq.filter (fun item -> item.IsLiveAt(_impl.Database.CurrentRevisionId))

  let _insert (model: 'm) =
    assert (model |> Model.hasId |> not)
    let record = model |> Model.toRecord _modelType
    match _impl.Insert([| record |]) with
    | Pass [| recordId |] ->
      model.Id <- recordId
    | _ ->
      ArgumentException() |> raise

  let _remove recordId =
    match _impl.Remove([| recordId |]) with
    | Pass () -> ()
    | _ -> ArgumentException() |> raise

  override this.Id =
    _impl.Id

  override this.Name =
    _impl.Schema.Name

  override this.Drop() =
    _impl.Drop()

  override this.Item
    with get (recordId) =
      _item recordId

  override this.AllItems =
    _allItems ()

  override this.Items =
    _items ()

  override this.CountAllRecords =
    _impl.CountAllRecords

  override this.Insert(model) =
    _insert model

  override this.Remove(recordId) =
    _remove recordId
