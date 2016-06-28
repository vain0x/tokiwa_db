namespace TokiwaDb.Core

open System
open System.IO
open FsYaml

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Database =
  let transact (f: unit -> 'x) (this: Database) =
    let () =
      this.Transaction.Begin()
    in
      try
        let x = f ()
        let () = this.Transaction.Commit()
        in x
      with
      | _ ->
        this.Transaction.Rollback()
        reraise ()

type RepositoryDatabaseConfig =
  {
    CurrentRevision: RevisionId
  }

type RepositoryDatabase(_repo: Repository) as this =
  inherit ImplDatabase()

  let _tableRepo =
    _repo.AddSubrepository("tables")

  let _storageSource =
    _repo.Add("storage")

  let _storageHashTableSource =
    _repo.Add("storage.ht_index")

  let _storage =
    StreamSourceStorage(_storageSource, _storageHashTableSource)

  let _configSource =
    _repo.Add("config.yaml")

  let _config =
    _configSource.ReadString()
    |> Yaml.tryLoad<RepositoryDatabaseConfig>

  let _revisionServer =
    let currentRevision =
      match _config with
      | Some config -> config.CurrentRevision
      | None -> 0L
    in
      MemoryRevisionServer(currentRevision)

  let _saveConfig () =
    let config =
      {
        CurrentRevision     = _revisionServer.Current
      }
    in _configSource.WriteString(config |> Yaml.dump)

  let _tables: ResizeArray<ImplTable> =
    _tableRepo.AllSubrepositories()
    |> Seq.map (fun repo ->
      RepositoryTable(this, repo.Name |> int64, repo) :> ImplTable
      )
    |> ResizeArray.ofSeq

  let _transaction = MemoryTransaction(this.Perform, _revisionServer) :> ImplTransaction

  let _createTable schema =
    lock _transaction.SyncRoot (fun () ->
      let tableId         = _tables.Count |> int64
      let revisionId      = _revisionServer.Increase()
      let repo            = _tableRepo.AddSubrepository(string tableId)
      let table           = RepositoryTable.Create(this, tableId, repo, schema, revisionId) :> ImplTable
      /// Add table.
      _tables.Add(table)
      /// Return the new table.
      table
      )

  let _perform operations =
    for operation in operations do
      match operation with
      | InsertRecords (tableId, records) ->
        _tables.[int tableId].PerformInsert(records)
      | RemoveRecords (tableId, recordIds) ->
        _tables.[int tableId].PerformRemove(recordIds)
      | DropTable (tableId) ->
        _tables.[int tableId].PerformDrop()

  let mutable _isDisposed = false

  let _dispose () =
    if not _isDisposed then
      _isDisposed <- true
      _saveConfig ()

  interface IDisposable with
    override this.Dispose() =
      _dispose ()

  override this.Name =
    _repo.Name

  override this.CurrentRevisionId =
    _transaction.RevisionServer.Current

  override this.Transaction =
    _transaction :> Transaction

  override this.ImplTransaction =
    _transaction

  override this.Storage =
    _storage :> Storage

  override this.ImplTables =
    _tables :> seq<ImplTable>

  override this.CreateTable(schema: TableSchema) =
    _createTable schema

  override this.Perform(operations) =
    _perform operations

type MemoryDatabase(_name: string) =
  inherit RepositoryDatabase(MemoryRepository(_name))

type DirectoryDatabase(_dir: DirectoryInfo) =
  inherit RepositoryDatabase(FileSystemRepository(_dir))
