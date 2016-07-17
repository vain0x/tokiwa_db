namespace TokiwaDb.CodeFirst.Detail

[<AutoOpen>]
module ExpressionExtensions =
  open System
  open System.Linq.Expressions

  type Expression with
    static member OfFunUnit(expr: Expression<Func<_>>) =
      expr

    static member OfFun(expr: Expression<Func<_, _>>) =
      expr

module Expression =
  open System.Linq.Expressions

  let rec tryMemberInfo (expr: Expression) =
    match expr.NodeType with
    | ExpressionType.Lambda ->
      (expr :?> LambdaExpression).Body |> tryMemberInfo
    | ExpressionType.Convert ->
      (expr :?> UnaryExpression).Operand |> tryMemberInfo
    | ExpressionType.MemberAccess ->
      (expr :?> MemberExpression).Member |> Some
    | _ -> None

  let call methodInfo (arguments: seq<_>) expr =
    Expression.Call(expr, methodInfo, arguments)

  let constant value =
    Expression.Constant(value)

  let constantOf typ value =
    Expression.Constant(value, typ)

  let convert typ expr =
    Expression.Convert(expr, typ)

  let invoke (arguments: seq<Expression>) expr =
    Expression.Invoke(expr, arguments)

  let toLambda (parameters: seq<ParameterExpression>) expr =
    Expression.Lambda(expr, parameters)

  module Lambda =
    let compile (expr: LambdaExpression) =
      expr.Compile()

module FSharpValue =
  open System
  open Microsoft.FSharp.Reflection

  module Lazy =
    let ofClosure elementType f =
      let funcType      = typedefof<Func<_>>.MakeGenericType([| elementType |])
      let funValue      = FSharpValue.MakeFunction(typeof<unit -> obj>, fun x -> f () :> obj)
      let func          =
        funValue
        |> Expression.constant
        |> Expression.call
          (typeof<unit -> obj>.GetMethod("Invoke"))
          [| () |> Expression.constantOf typeof<unit> |]
        |> Expression.convert elementType
        |> Expression.toLambda Seq.empty
        |> Expression.Lambda.compile
      let lazyType      = typedefof<Lazy<_>>.MakeGenericType([| elementType |])
      let lazyValue     = lazyType.GetConstructor([| funcType |]).Invoke([| func |])
      in lazyValue
