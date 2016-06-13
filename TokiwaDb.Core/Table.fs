namespace TokiwaDb.Core

open System
open System.IO

type Table(_name: Name, _schema: Schema, _relation: IRelation) =
  interface ITable with
    override this.Name = _name
    override this.Schema = _schema
    override this.Relation = _relation
