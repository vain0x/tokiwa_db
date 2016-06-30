namespace TokiwaDb.CodeFirst

open TokiwaDb.Core

type IModel =
  abstract member Id: RecordId with get, set

type [<AbstractClass>] Model<'m when 'm :> IModel>() =
  member val Id = -1L with get, set

  interface IModel with
    override this.Id
      with get () = this.Id
      and  set v  = this.Id <- v

type [<AbstractClass>] Table<'m when 'm :> IModel>() =
  inherit BaseTable()

  abstract member Item: RecordId -> 'm
  abstract member Items: seq<'m>

  abstract member Insert: 'm -> unit
  abstract member Remove: RecordId -> unit

  // TODO: Implement IMortal.

type [<AbstractClass>] Database() =
  inherit BaseDatabase()

  abstract member Table<'m when 'm :> IModel> : unit -> Table<'m>
