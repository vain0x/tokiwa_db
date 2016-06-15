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

  let _configFile =
    FileInfo(Path.Combine(_dir.FullName, ".config"))

  let _config =
    if _configFile.Exists then
      let configText = _configFile |> FileInfo.readText
      in configText |> Yaml.tryLoad<FileDatabaseConfig>
    else None

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
    File.WriteAllText(_configFile.FullName, Yaml.dump config)

  let mutable _tables =
      _tableDir.GetFiles("*.schema")
      |> Array.map (fun schemaFile ->
        let tableFile   = FileInfo(Path.ChangeExtension(schemaFile.FullName, ".table"))
        if tableFile.Exists then
          schemaFile |> FileInfo.readText
          |> Yaml.tryLoad<Mortal<Schema>>
          |> Option.map (fun schema ->
            schema |> Mortal.map (fun schema ->
              let name              = Path.GetFileNameWithoutExtension(tableFile.Name)
              let streamSource      = WriteOnceFileStreamSource(tableFile)
              in StreamTable(this, name, schema, streamSource)
              ))
        else None
        )
      |> Array.choose id
      |> Array.toList

  let mutable _isDisposed = false

  interface IDisposable with
    override this.Dispose() =
      if not _isDisposed then
        _isDisposed <- true
        _saveConfig ()

  override this.Finalize() =
    (this :> IDisposable).Dispose()

  override this.SyncRoot =
    _syncRoot

  override this.Name =
    _dir.Name

  override this.RevisionServer =
    _revisionServer :> RevisionServer

  override this.Storage =
    _storage :> Storage

  override this.Tables(t) =
    _tables |> Seq.choose (fun table ->
      if table |> Mortal.isAliveAt t
      then Some (table.Value :> Table)
      else None
      )

  override this.CreateTable(name: string, schema: Schema) =
    if _tables |> List.exists (fun table -> table.Value.Name = name) then
      failwithf "Table name '%s' has been already taken." name
    else
      let revisionId      = _revisionServer.Next()
      /// Create schema file.
      let mortalSchema    = schema |> Mortal.create revisionId
      let schemaFile      = FileInfo(Path.Combine(_tableDir.FullName, name + ".schema"))
      use schemaStream    = schemaFile.CreateText()
      schemaStream.Write(mortalSchema |> Yaml.dump)
      /// Create table file.
      let tableFile       = FileInfo(Path.Combine(_tableDir.FullName, name + ".table"))
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
      let revisionId      = _revisionServer.Next()
      /// Kill schema.
      let mortalSchema    = table |> Mortal.map (fun table -> table.Schema) |> Mortal.kill revisionId
      let schemaFile      = FileInfo(Path.Combine(_tableDir.FullName, name + ".schema"))
      use stream          = schemaFile.CreateText()
      stream.Write(mortalSchema |> Yaml.dump)
      /// Kill table.
      _tables <- (table |> Mortal.kill revisionId) :: tables'
      /// Return true, which indicates some table is dropped.
      true
