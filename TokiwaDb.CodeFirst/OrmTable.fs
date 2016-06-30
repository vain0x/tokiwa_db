namespace TokiwaDb.CodeFirst.Detail

open System
open Chessie.ErrorHandling
open TokiwaDb.Core
open TokiwaDb.CodeFirst

type OrmTable<'m when 'm :> IModel>(_impl: ImplTable) =
  inherit Table<'m>()

  let _toModel (rp: MortalValue<RecordPointer>) =
    rp |> MortalValue.map _impl.Database.Storage.Derefer
    |> Model.ofMortalRecord<'x>

  let _item recordId =
    match _impl.RecordById(recordId) with
    | Some recordPointer ->
      recordPointer |> _toModel
    | None ->
      ArgumentOutOfRangeException() |> raise

  let _items () =
    _impl.RecordPointers
    |> Seq.map _toModel

  let _insert model =
    assert (model |> Model.hasId |> not)
    let record = model |> Model.toRecord<'m>
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

  override this.Item(recordId) =
    _item recordId

  override this.Items =
    _items ()

  override this.CountAllRecords =
    _impl.CountAllRecords

  override this.Insert(model) =
    _insert model

  override this.Remove(recordId) =
    _remove recordId
