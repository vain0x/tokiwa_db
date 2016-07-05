namespace TokiwaDb.CodeFirst.Detail

[<AutoOpen>]
module ExpressionExtensions =
  open System
  open System.Linq.Expressions

  type Expression with
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
