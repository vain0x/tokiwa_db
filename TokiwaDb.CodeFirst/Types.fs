namespace TokiwaDb.CodeFirst

open System
open TokiwaDb.Core

type IModel =
  abstract member Id: RecordId with get, set
  abstract member IsLiveAt: RevisionId -> bool

  abstract member Birth: int64 with get, set
  abstract member Death: int64 with get, set

type [<AbstractClass>] Model() =
  member val Id = -1L with get, set

  member this.IsLiveAt(t) =
    this.Birth <= t && t < this.Death

  member val internal Birth = Int64.MaxValue with get, set
  member val internal Death = Int64.MaxValue with get, set

  interface IModel with
    override this.Id
      with get () = this.Id
      and  set v  = this.Id <- v

    override this.IsLiveAt(t) =
      this.IsLiveAt(t)

    override this.Birth
      with get () = this.Birth
      and  set v  = this.Birth <- v

    override this.Death
      with get () = this.Death
      and  set v  = this.Death <- v

type [<AbstractClass>] Table<'m when 'm :> IModel>() =
  inherit BaseTable()

  abstract member Item: RecordId -> 'm with get
  abstract member AllItems: seq<'m>
  abstract member Items: seq<'m>

  abstract member Insert: 'm -> unit
  abstract member Remove: RecordId -> unit

type [<AbstractClass>] Database() =
  inherit BaseDatabase()
