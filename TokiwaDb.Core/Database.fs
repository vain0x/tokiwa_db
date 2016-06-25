namespace TokiwaDb.Core

open System
open System.IO
open FsYaml

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Database =
  let tryFindLivingTable name (this: Database) =
      this.TryFindLivingTable(name, this.CurrentRevisionId)

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
    let schemaSource    = repo.TryFind(".schema") |> Option.get
    let mortalSchema    = schemaSource.ReadString() |> Yaml.load<Mortal<TableSchema>>
    let schema          = mortalSchema.Value
    let indexes         = _loadIndexes repo schema
    let tableSource     = repo.TryFind(".table") |> Option.get
    let table           = StreamTable(this, schema, indexes, tableSource) :> Table
    let mortalTable     = mortalSchema |> Mortal.map (fun _ -> table)
    in (repo.Name, mortalTable)

  let mutable _tables: Map<string, Mortal<Table>> =
    _tableRepo.AllSubrepositories()
    |> Seq.map _loadTable
    |> Map.ofSeq

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
    _tables |> Seq.choose (fun (KeyValue (_, table)) ->
      if table |> Mortal.isAliveAt t
      then Some table.Value
      else None
      )

  override this.TryFindLivingTable(tableName, t) =
    _tables |> Map.tryFind tableName
    |> Option.bind (Mortal.valueIfAliveAt t)

  override this.CreateTable(schema: TableSchema) =
    lock this.Transaction.SyncRoot (fun () ->
      let name = schema.Name
      if _tables |> Map.containsKey name then
        failwithf "Table name '%s' has been already taken." name
      else
        let revisionId      = _revisionServer.Increase()
        let repo            = _tableRepo.AddSubrepository(name)
        /// Create schema file.
        let mortalSchema    = schema |> Mortal.create revisionId
        let schemaSource    = repo.Add(".schema")
        let ()              = schemaSource.WriteString(mortalSchema |> Yaml.dump)
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
        let table           = StreamTable(this, schema, indexes, tableSource) :> Table
        /// Add table.
        _tables <- _tables |> Map.add table.Name (mortalSchema |> Mortal.map (fun _ -> table))
        /// Return the new table.
        table
      )

  override this.DropTable(name: string) =
    lock this.Transaction.SyncRoot (fun () ->
      match _tables |> Map.tryFind name with
      | Some table ->
        let revisionId      = _revisionServer.Increase()
        /// Kill schema.
        let mortalSchema    = table |> Mortal.map (fun table -> table.Schema) |> Mortal.kill revisionId
        let ()              =
          _tableRepo.TryFindSubrepository(name)
          |> Option.bind (fun repo ->
            repo.TryFind(".schema")
            )
          |> Option.iter (fun schemaSource ->
            schemaSource.WriteString(mortalSchema |> Yaml.dump)
            )
        /// Kill table.
        _tables <- _tables |> Map.add name (table |> Mortal.kill revisionId)
        /// Return true, which indicates some table is dropped.
        true
      | None ->
        false
      )

  override this.Perform(operations) =
    for operation in operations do
      match operation with
      | InsertRecords (tableName, records) ->
        this |> Database.tryFindLivingTable tableName
        |> Option.iter (fun table -> table.PerformInsert(records))
      | RemoveRecords (tableName, recordIds) ->
        this |> Database.tryFindLivingTable tableName
        |> Option.iter (fun table -> table.PerformRemove(recordIds))

type MemoryDatabase(_name: string) =
  inherit RepositoryDatabase(MemoryRepository(_name))
