namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module DatabaseTest =
  open TokiwaDb.Core.Test.TableTest.TestData

  let createTest =
    test {
      let testDb =
        StreamDatabase(@"testDb")
      let persons = persons ()
      testDb.CreateTable()
    }

