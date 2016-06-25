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
  inherit Database()

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

  let _loadIndexes (repo: Repository) (schema: TableSchema) =
    schema.Indexes |> Array.mapi (fun i indexSchema ->
      match indexSchema with
      | HashTableIndexSchema fieldIndexes ->
        repo.TryFind(sprintf "%d.ht_index" i)
        |> Option.get
        |> (fun source ->
          StreamHashTableIndex(fieldIndexes, source) :> HashTableIndex
          ))

  let _loadTable (repo: Repository) =
    let tableId         = Int64.Parse(repo.Name)
    let schemaSource    = repo.TryFind(".schema") |> Option.get
    let schema          = schemaSource.ReadString() |> FsYaml.customLoad<TableSchema>
    let indexes         = _loadIndexes repo schema
    let tableSource     = repo.TryFind(".table") |> Option.get
    let table           = StreamTable(this, tableId, schema, indexes, tableSource) :> Table
    in table

  let _tables: ResizeArray<Table> =
    _tableRepo.AllSubrepositories()
    |> Seq.map _loadTable
    |> ResizeArray.ofSeq

  let _transaction = MemoryTransaction(this.Perform, _revisionServer) :> Transaction

  let mutable _isDisposed = false

  interface IDisposable with
    override this.Dispose() =
      if not _isDisposed then
        _isDisposed <- true
        _saveConfig ()

  override this.Name =
    _repo.Name

  override this.Transaction =
    _transaction

  override this.Storage =
    _storage :> Storage

  override this.Tables(t) =
    _tables |> Seq.choose (fun table ->
      if table |> Mortal.isAliveAt t
      then Some table
      else None
      )

  override this.CreateTable(schema: TableSchema) =
    lock this.Transaction.SyncRoot (fun () ->
      let tableId         = _tables.Count |> int64
      let revisionId      = _revisionServer.Increase()
      let repo            = _tableRepo.AddSubrepository(string tableId)
      // Get born.
      let schema          = { schema with LifeSpan = schema.LifeSpan |> Mortal.isBorn revisionId }
      /// Create schema file.
      let schemaSource    = repo.Add(".schema")
      let ()              = schemaSource.WriteString(schema |> FsYaml.customDump)
      /// Create index files.
      let indexes         =
        schema.Indexes |> Array.mapi (fun i indexSchema ->
          match indexSchema with
          | HashTableIndexSchema fieldIndexes ->
            let indexSource   = repo.Add(sprintf "%d.ht_index" i)
            in StreamHashTableIndex(fieldIndexes, indexSource) :> HashTableIndex
          )
      /// Create table file.
      let tableSource     = repo.Add(".table")
      let table           = StreamTable(this, tableId, schema, indexes, tableSource) :> Table
      /// Add table.
      _tables.Add(table)
      /// Return the new table.
      table
      )

  override this.DropTable(tableId) =
    lock this.Transaction.SyncRoot (fun () ->
      if 0L <= tableId && tableId < (_tables.Count |> int64) then
        let table           = _tables.[int tableId]
        let revisionId      = _revisionServer.Increase()
        // Kill table.
        let schema          =
          { table.Schema with LifeSpan = table.Schema.LifeSpan |> Mortal.kill revisionId }
        let () =
          table.SetSchema(schema)
        let ()              =
          _tableRepo.TryFindSubrepository(string tableId)
          |> Option.bind (fun repo ->
            repo.TryFind(".schema")
            )
          |> Option.iter (fun schemaSource ->
            schemaSource.WriteString(schema |> FsYaml.customDump)
            )
        /// Return true, which indicates some table is dropped.
        true
      else
        false
      )

  override this.Perform(operations) =
    for operation in operations do
      match operation with
      | InsertRecords (tableId, records) ->
        _tables.[int tableId].PerformInsert(records)
      | RemoveRecords (tableId, recordIds) ->
        _tables.[int tableId].PerformRemove(recordIds)

type MemoryDatabase(_name: string) =
  inherit RepositoryDatabase(MemoryRepository(_name))
