namespace TokiwaDb.Core.Test

open TokiwaDb.Core

type NullTransaction private() =
  inherit Transaction()

  override this.Begin() = ()
  override this.Commit() = ()
  override this.Rollback() = ()

  static member val Instance =
    NullTransaction() :> Transaction
