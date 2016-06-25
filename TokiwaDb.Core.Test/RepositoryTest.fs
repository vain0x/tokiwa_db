namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module RepositoryTest =
  let repositoryTest (repo: Repository) =
    [
      test {
        let _ = repo.AddSubrepository("sub")
        do! repo.TryFindSubrepository("sub")
          |> Option.map (fun sub -> sub.Name)
          |> assertEquals (Some "sub")
      }
      test {
        let _ = repo.Add("x0.x")
        let _ = repo.Add("x1.x")
        let _ = repo.Add("y0.y")
        do! repo.TryFind("x0.x") |> assertSatisfies Option.isSome
        do! repo.TryFind("____") |> assertEquals None
        do! repo.FindManyBySuffix(".x")
          |> Seq.map fst
          |> Seq.toArray
          |> assertEquals [|"x0.x"; "x1.x"|]
      }
    ]

  let memoryRepositoryTest =
    MemoryRepository("mem_repo")
    |> repositoryTest
