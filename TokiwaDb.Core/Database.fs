namespace TokiwaDb.Core

open System
open System.IO
open FsYaml

type MemoryDatabase(_name: string, _rev: RevisionServer, _storage: Storage, _tables: list<Mortal<Table>>) =
  inherit Database()

  let _syncRoot = new obj()

  let mutable _tables = _tables

  new (name: string) =
    MemoryDatabase(name, MemoryRevisionServer() :> RevisionServer, new MemoryStorage(), [])

  override this.SyncRoot = _syncRoot

  override this.Name = _name

  override this.RevisionServer =
    _rev

  override this.Storage =
    _storage

  override this.Tables(t) =
    _tables |> Seq.choose (fun table ->
      if table |> Mortal.isAliveAt t
      then Some table.Value
      else None
      )

  override this.CreateTable(name, schema) =
    let revisionId =
      _rev.Next()
    let table =
      StreamTable(this :> Database, name, schema, new MemoryStreamSource()) :> Table
    let () =
      _tables <- (table |> Mortal.create revisionId) :: _tables
    in table

  override this.DropTable(name) =
    let revisionId =
      _rev.Next()
    let (droppedTables, tables') =
      _tables |> List.partition (fun table -> table.Value.Name = name)
    let droppedTables' =
      droppedTables |> List.map (Mortal.kill revisionId)
    let () =
      _tables <- droppedTables' @ tables'
    in
      droppedTables |> List.isEmpty |> not

type FileDatabaseConfig =
  {
    CurrentRevision: RevisionId
  }

type DirectoryDatabase(_dir: DirectoryInfo) as this =
  inherit Database()

  let _syncRoot = new obj()

  let _tableDir = DirectoryInfo(Path.Combine(_dir.FullName, ".table"))
  let _storageFile = FileInfo(Path.Combine(_dir.FullName, ".storage"))

  do _dir.Create()
  do _tableDir.Create()
  do
    if not _storageFile.Exists then
      _storageFile |> FileInfo.createNew

  let _storage =
    StreamSourceStorage(WriteOnceFileStreamSource(_storageFile))

  let mutable _tables =
    _tableDir.GetFiles("*.schema")
    |> Array.map (fun file ->
      async {
        let tableFile   = FileInfo(Path.ChangeExtension(file.FullName, ".table"))
        if tableFile.Exists then
          let! text       = file |> FileInfo.readTextAsync
          return 
            text |> Yaml.tryLoad<Mortal<Schema>> |> Option.map (fun schema ->
              schema |> Mortal.map (fun schema ->
                StreamTable(this, tableFile.Name, schema, WriteOnceFileStreamSource(tableFile))
                ))
        else
          return None
      })
    |> Async.Parallel
    |> Async.RunSynchronously
    |> Array.choose id
    |> Array.toList

  let configFile =
    FileInfo(Path.Combine(_dir.FullName, ".config"))

  let config =
    if configFile.Exists then
      let configText = configFile |> FileInfo.readTextAsync |> Async.RunSynchronously
      in configText |> Yaml.tryLoad<FileDatabaseConfig>
    else None

  let revisionServer =
    let currentRevision =
      match config with
      | Some config -> config.CurrentRevision
      | None -> 0L
    in
      MemoryRevisionServer(currentRevision)

  override this.SyncRoot =
    _syncRoot

  override this.Name =
    _dir.Name

  override this.RevisionServer =
    revisionServer :> RevisionServer

  override this.Storage =
    _storage :> Storage

  override this.Tables(t) =
    _tables |> Seq.choose (fun table ->
      if table |> Mortal.isAliveAt t
      then Some (table.Value :> Table)
      else None
      )

  override this.CreateTable(name: string, schema: Schema) =
    match _tables |> List.tryFind (fun table -> table.Value.Name = name) with
    | None ->
      failwithf "Table name '%s' has been already taken." name
    | Some table ->
      let revisionId = revisionServer.Next()
      /// Create schema file.
      let mortalSchema    = schema |> Mortal.create revisionId
      let schemaFile      = FileInfo(Path.Combine(_dir.FullName, name + ".schema"))
      use schemaStream    = schemaFile.CreateText()
      schemaStream.Write(mortalSchema |> Yaml.dump)
      /// Create table file.
      let tableFile       = FileInfo(Path.Combine(_dir.FullName, name + ".table"))
      let tableSource     = WriteOnceFileStreamSource(tableFile)
      let table           = StreamTable(this, name, schema, tableSource)
      tableFile |> FileInfo.createNew
      /// Add table.
      _tables <- (mortalSchema |> Mortal.map (fun _ -> table)) :: _tables
      /// Return the new table.
      table :> Table

  override this.DropTable(name: string) =
    match _tables |> List.partition (fun table -> table.Value.Name = name) with
    | ([], _) ->
      false
    | (table :: _, tables') ->
      let revisionId = revisionServer.Next()
      /// Kill schema.
      let mortalSchema    = table |> Mortal.map (fun table -> table.Schema) |> Mortal.kill revisionId
      let schemaFile      = FileInfo(Path.Combine(_dir.FullName, name + ".schema"))
      use stream          = schemaFile.CreateText()
      stream.Write(mortalSchema |> Yaml.dump)
      /// Kill table.
      _tables <- (table |> Mortal.kill revisionId) :: tables'
      /// Return true, which indicates some table is dropped.
      true
