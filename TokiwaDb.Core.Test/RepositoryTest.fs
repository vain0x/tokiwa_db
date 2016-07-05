namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module RepositoryTest =
  let repositoryTest (repo: Repository) =
    [
      test {
        let _ = repo.AddSubrepository("sub")
        let _ = repo.AddSubrepository("sub2")
        do! repo.TryFindSubrepository("sub")
          |> Option.map (fun sub -> sub.Name)
          |> assertEquals (Some "sub")
        do! repo.AllSubrepositories()
          |> Array.map (fun repo -> repo.Name)
          |> assertEquals [|"sub"; "sub2"|]
      }
      test {
        let _ = repo.Add("x0.x")
        let _ = repo.Add("y0.y")
        do! repo.TryFind("x0.x") |> assertSatisfies Option.isSome
        do! repo.TryFind("____") |> assertEquals None
      }
    ]

  let memoryRepositoryTest =
    MemoryRepository("mem_repo")
    |> repositoryTest
