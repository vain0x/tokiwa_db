﻿namespace TokiwaDb.Core

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

  let _loadIndexes (schema: TableSchema) =
    schema.Indexes |> Array.mapi (fun i indexSchema ->
      match indexSchema with
      | HashTableIndexSchema fieldIndexes ->
        _tableRepo.TryFind(sprintf "%s.%d.ht_index" schema.Name i)
        |> Option.map (fun source ->
          StreamHashTableIndex(fieldIndexes, source) :> HashTableIndex
          ))
    |> Array.choose id

  let _tryLoadTable (schemaName: string, schemaSource: StreamSource) =
    let name            = Path.GetFileNameWithoutExtension(schemaName)
    in
      schemaSource.ReadString()
      |> Yaml.tryLoad<Mortal<TableSchema>>
      |> Option.bind (fun mortalSchema ->
        _tableRepo.TryFind(name + ".table")
        |> Option.map (fun tableSource ->
            let schema      = mortalSchema.Value
            let indexes     = _loadIndexes schema
            let table       = StreamTable(this, schema, indexes, tableSource) :> Table
            in mortalSchema |> Mortal.map (fun _ -> table)
            ))

  let mutable _tables: Map<string, Mortal<Table>> =
    _tableRepo.FindManyBySuffix(".schema")
    |> Seq.choose _tryLoadTable
    |> Seq.map (fun mortalTable ->
      (mortalTable.Value.Name, mortalTable))
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
        /// Create schema file.
        let mortalSchema    = schema |> Mortal.create revisionId
        let schemaSource    = _tableRepo.Add(name + ".schema")
        let ()              = schemaSource.WriteString(mortalSchema |> Yaml.dump)
        /// Create index files.
        let indexes         =
          schema.Indexes |> Array.mapi (fun i indexSchema ->
            match indexSchema with
            | HashTableIndexSchema fieldIndexes ->
              let indexSource   = _tableRepo.Add(sprintf "%s.%d.ht_index" name i)
              in StreamHashTableIndex(fieldIndexes, indexSource) :> HashTableIndex
            )
        /// Create table file.
        let tableSource     = _tableRepo.Add(name + ".table")
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
          _tableRepo.TryFind(name + ".schema")
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
