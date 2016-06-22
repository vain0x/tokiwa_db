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

  override this.CreateTable(schema, indexSchemas) =
    let revisionId =
      _rev.Next()
    let indexes =
      indexSchemas |> Array.map
        (function
        | HashTableIndexSchema fieldIndexes ->
          StreamHashTableIndex(fieldIndexes, new MemoryStreamSource()) :> HashTableIndex
        )
    let table =
      StreamTable(this :> Database, schema, indexes, new MemoryStreamSource()) :> Table
    let () =
      _tables <- (table |> Mortal.create revisionId) :: _tables
    in table

  override this.CreateTable(schema) =
    this.CreateTable(schema, [||])

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
    FileStorage(_storageFile)

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
        let indexesFile = FileInfo(Path.ChangeExtension(schemaFile.FullName, ".indexes"))
        if tableFile.Exists then
          schemaFile |> FileInfo.readText
          |> Yaml.tryLoad<Mortal<TableSchema>>
          |> Option.map (fun schema ->
            schema |> Mortal.map (fun schema ->
              let name              = Path.GetFileNameWithoutExtension(tableFile.Name)
              let fieldIndexesList  =
                indexesFile |> FileInfo.readText
                |> Yaml.load<array<array<int>>>
              let indexes           =
                fieldIndexesList |> Array.mapi (fun i fieldIndexes ->
                  let file = FileInfo(Path.Combine(_tableDir.FullName, sprintf "%s.%d.ht_index" name i))
                  in StreamHashTableIndex(fieldIndexes, FileStreamSource(file)))
              let streamSource      = FileStreamSource(tableFile)
              in StreamTable(this, schema, streamSource)
              ))
        else None
        )
      |> Array.choose id
      |> Array.map (fun table -> (table.Value.Name, table))
      |> Map.ofArray

  let mutable _isDisposed = false

  interface IDisposable with
    override this.Dispose() =
      if not _isDisposed then
        _isDisposed <- true
        _saveConfig ()

  override this.SyncRoot =
    _syncRoot

  override this.Name =
    _dir.Name

  override this.RevisionServer =
    _revisionServer :> RevisionServer

  override this.Storage =
    _storage :> Storage

  override this.Tables(t) =
    _tables |> Seq.choose (fun (KeyValue (_, table)) ->
      if table |> Mortal.isAliveAt t
      then Some (table.Value :> Table)
      else None
      )

  override this.CreateTable(schema: TableSchema, indexSchemas) =
    let name = schema.Name
    if _tables |> Map.containsKey name then
      failwithf "Table name '%s' has been already taken." name
    else
      let revisionId      = _revisionServer.Next()
      /// Create schema file.
      let mortalSchema    = schema |> Mortal.create revisionId
      let schemaFile      = FileInfo(Path.Combine(_tableDir.FullName, name + ".schema"))
      use schemaStream    = schemaFile.CreateText()
      schemaStream.Write(mortalSchema |> Yaml.dump)
      /// Create index files.
      let indexes         =
        indexSchemas |> Array.mapi (fun i indexSchema ->
          match indexSchema with
          | HashTableIndexSchema fieldIndexes ->
            let indexFile   = FileInfo(Path.Combine(_tableDir.FullName, sprintf "%s.%d.ht_index" name i))
            let ()          = indexFile |> FileInfo.createNew
            in StreamHashTableIndex(fieldIndexes, FileStreamSource(indexFile))
          )
      let indexesFile     = FileInfo(Path.Combine(_tableDir.FullName, name + ".indexes"))
      let ()              = File.WriteAllText(indexesFile.FullName, Yaml.dump indexSchemas)
      /// Create table file.
      let tableFile       = FileInfo(Path.Combine(_tableDir.FullName, name + ".table"))
      let tableSource     = FileStreamSource(tableFile)
      let table           = StreamTable(this, schema, tableSource)
      tableFile |> FileInfo.createNew
      /// Add table.
      _tables <- _tables |> Map.add table.Name (mortalSchema |> Mortal.map (fun _ -> table))
      /// Return the new table.
      table :> Table

  override this.CreateTable(schema: TableSchema) =
    this.CreateTable(schema, [||])

  override this.DropTable(name: string) =
    match _tables |> Map.tryFind name with
    | Some table ->
      let revisionId      = _revisionServer.Next()
      /// Kill schema.
      let mortalSchema    = table |> Mortal.map (fun table -> table.Schema) |> Mortal.kill revisionId
      let schemaFile      = FileInfo(Path.Combine(_tableDir.FullName, name + ".schema"))
      use stream          = schemaFile.CreateText()
      stream.Write(mortalSchema |> Yaml.dump)
      /// Kill table.
      _tables <- _tables |> Map.add name (table |> Mortal.kill revisionId)
      /// Return true, which indicates some table is dropped.
      true
    | None ->
      false
