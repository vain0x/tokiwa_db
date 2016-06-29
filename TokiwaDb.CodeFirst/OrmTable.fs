namespace TokiwaDb.CodeFirst.Detail

open System
open Chessie.ErrorHandling
open TokiwaDb.Core
open TokiwaDb.CodeFirst

type OrmTable<'m when 'm :> IModel>(_impl: ImplTable) =
  inherit Table<'m>()

  let _toModel (rp: RecordPointer) =
    _impl.Database.Storage.Derefer(rp) |> Model.ofRecord<'x>

  let _item recordId =
    match _impl.RecordById(recordId) with
    | Some recordPointer ->
      recordPointer.Value |> _toModel
    | None ->
      ArgumentOutOfRangeException() |> raise

  let _items () =
    _impl.RecordPointers
    |> Seq.map (fun rp -> rp.Value |> _toModel)

  let _insert model =
    assert (model |> Model.hasId |> not)
    let record = model |> Model.toRecord<'m>
    match _impl.Insert([| record |]) with
    | Pass [| recordId |] ->
      model.Id <- recordId
    | _ ->
      ArgumentException() |> raise

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

  override this.Insert(model) =
    _insert model
