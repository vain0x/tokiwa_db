namespace TokiwaDb.Core

open System.Collections.Generic
open System.IO

type [<AbstractClass>] Repository() =
  abstract member Name: string
  abstract member FullName: string

  abstract member AddSubrepository: string -> Repository
  abstract member TryFindSubrepository: string -> option<Repository>

  abstract member Add: string -> StreamSource
  abstract member TryFind: string -> option<StreamSource>
  abstract member FindManyBySuffix: string -> seq<string * StreamSource>

type MemoryRepository(_name: string) =
  inherit Repository()

  let _repositories = Dictionary<string, Repository>()

  let _items = Dictionary<string, StreamSource>()

  override this.Name = _name

  override this.FullName = _name

  override this.AddSubrepository(name) =
    let repo      = MemoryRepository(name) :> Repository
    let _         = _repositories.Add(name, repo)
    in repo

  override this.TryFindSubrepository(name) =
    _repositories.TryGetValue(name) |> Option.ofPair

  override this.Add(name) =
    let item      = MemoryStreamSource() :> StreamSource
    let _         = _items.Add(name, item)
    in item

  override this.TryFind(name) =
    _items.TryGetValue(name) |> Option.ofPair

  override this.FindManyBySuffix(suffix) =
    _items |> Seq.choose (fun (KeyValue (name, item)) ->
      if name.EndsWith(suffix)
      then Some (name, item)
      else None
      )

type FileSystemRepository(_dir: DirectoryInfo) =
  inherit Repository()

  let _subdir name = DirectoryInfo(Path.Combine(_dir.FullName, name))

  let _subrepo (subdir: DirectoryInfo) =
    FileSystemRepository(subdir) :> Repository

  let _subfile name = FileInfo(Path.Combine(_dir.FullName, name))

  let _subitem (subfile: FileInfo) =
    FileStreamSource(subfile) :> StreamSource

  do _dir.Create()

  override this.Name = _dir.Name

  override this.FullName = _dir.FullName

  override this.AddSubrepository(name) =
    let subdir      = _subdir name
    let ()          = subdir.Create()
    let subrepo     = _subrepo subdir
    in subrepo

  override this.TryFindSubrepository(name) =
    _dir.GetDirectories(name)
    |> Array.tryHead
    |> Option.map _subrepo

  override this.Add(name) =
    let subfile     = _subfile name
    let ()          =
      if File.Exists(subfile.FullName) |> not then
        subfile |> FileInfo.createNew
    in _subitem subfile

  override this.TryFind(name) =
    _dir.GetFiles(name)
    |> Array.tryHead
    |> Option.map _subitem

  override this.FindManyBySuffix(suffix) =
    _dir.GetFiles()
    |> Seq.choose (fun subfile ->
      if subfile.Name.EndsWith(suffix)
      then (subfile.Name, subfile |> _subitem) |> Some
      else None
      )
