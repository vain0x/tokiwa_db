namespace TokiwaDb.CodeFirst.Test

open System
open TokiwaDb.Core
open TokiwaDb.Core.Test
open TokiwaDb.CodeFirst

type EmptyTable<'m when 'm :> IModel>() =
  inherit Table<'m>()

  override this.Id              = 0L
  override this.Name            = "always_empty_table"
  override this.Drop()          = NotImplementedException() |> raise
  override this.CountAllRecords = 0L
  override this.Item with get _ = ArgumentException() |> raise
  override this.AllItems        = Seq.empty
  override this.Items           = Seq.empty
  override this.Insert(_)       = NotImplementedException() |> raise
  override this.Remove(_)       = ArgumentException() |> raise

type EmptyDatabase() =
  inherit Database()

  override this.Name                            = "always_empty_database"
  override this.CurrentRevisionId               = 0L
  override this.Transaction                     = NullTransaction.Instance
  override this.Table<'m when 'm :> IModel>()   =
    EmptyTable<'m>() :> Table<'m>
