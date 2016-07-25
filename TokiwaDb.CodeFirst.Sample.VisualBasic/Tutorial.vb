Imports System
Imports System.Collections.Generic
Imports System.Linq
Imports Microsoft.VisualStudio.TestTools.UnitTesting

' チュートリアル
' ===============================================

' 常磐DBを VisualBasic.NET から使用する方法の概略を説明します。

' ここでは、データベースを使用するコードだけでなく、
' データベースの作成や構成、接続に関することも
' すべて(VBの)コードを書くことで行います。
' (いわゆるコードファーストアプローチです。)

' 手順概略
' 0. プロジェクトに常磐DBライブラリーの参照を追加する。
' 1. データベースの「テーブル」に対応するクラス(モデルクラス)を定義する。
' 2. データベースにアクセスするためのインターフェイスとなるクラスを定義する。
' 3. データベースの作成(あるいは接続)と構成を行うコードを書く。
' 4. 各種データベース処理 (CR_D)

' -----------------------------------------------
' 0. 参照
' プロジェクトには次の2つのDLLを参照としてプロジェクトに追加しておく必要があります。
' - TokiwaDb.Core.dll
' - TokiwaDb.CodeFirst.dll

' -----------------------------------------------
' 1. モデルクラスの定義

' データベースにテーブルを作成するため、あるいは接続後にテーブルに参照するため、
' 以下のようなクラスを定義します。(これをモデルクラスと呼びます。いわゆるアクティブレコード。)

Public Class Person
    Inherits TokiwaDb.CodeFirst.Model

    ' 「public かつ書き込み可能なプロパティ」はテーブルのフィールドに対応します。
    ' SQL風にかくと「Name text NOT NULL」といったところ。
    Public Property Name As String

    ' フィールド: Age bigint NOT NULL
    Private _age As Long
    Public Property Age As Long
        Get
            Return _age
        End Get
        Set(value As Long)
            _age = value
        End Set
    End Property
End Class

' これもモデルクラスです。
Public Class Song
    Inherits Model

    Public Property Title As String

    ' プロパティを Lazy 型にしておくと、データベースからデータを取り出すとき、
    ' データの読み込みが必要になるときまで遅延されます。
    Public Property VocalName As System.Lazy(Of String)
End Class

' -----------------------------------------------
' 2. 文脈クラスの定義

' 上記で定義したような各モデルクラスに対して、
' 常磐DBは「テーブル」を表すインスタンスを生成します。
' それらのインスタンスを保持するためのクラスを1つ、定義しておく必要があります。
' これの定義は形式的に決まります。具体的には次の3つを並べたものです。
'    - Database プロパティ
'    - 各モデルクラス M に対して、Table<M> 型のプロパティ
'    - 各プロパティの値を設定するコンストラクター
' (これをデータベース文脈クラスと呼びます。)

Public Class TutorialDbContext
    ' データベース プロパティ
    Public Property Database As TokiwaDb.CodeFirst.Database

    ' 各モデルクラスに対応するテーブル プロパティ
    Public Property Persons As Table(Of Person)
    Public Property Songs As Table(Of Song)

    ' コンストラクター
    Public Sub New(database As Database, persons As Table(Of Person), songs As Table(Of Song))
        Me.Database = database
        Me.Persons = persons
        Me.Songs = songs
    End Sub
End Class

' -----------------------------------------------
' 3. データベースの作成・構成・接続

<TestClass>
Public Class Tutorial

    ' データベースを作成したり、既存のデータベースに接続したりするには、
    ' DbConfig クラスを使います。

    Public Function CreateDatabase() As TutorialDbContext
        Dim dbConfig = New TokiwaDb.CodeFirst.DbConfig(Of TutorialDbContext)()

        ' まずデータベースの構成 (テーブルにつける制約など) を記述します。
        ' 現在は一意性制約にのみ対応しています。

        ' (テーブル Person に一意性制約を追加するコード)
        dbConfig.Add(Of Person)(UniqueIndex.Of(Of Person)(Function(p) p.Name))

        ' 最後に OpenMemory メソッドを呼ぶことで、インメモリーデータベースを作成します。
        ' (インメモリーデータベースとは、メモリ内に生成されたデータベースです。
        '  ファイルとして保存されないので、プログラム終了時に消滅します。)
        Return dbConfig.OpenMemory("tutorial_db")
    End Function

    ' 通常のディスクベースのデータベースを生成するには、
    ' 代わりに OpenDatabase メソッドを使用します。
    ' (このチュートリアルではインメモリーデータベースを使用しますが、
    '  使い方はディスクベースのデータベースも同じです。)

    Public Function OpenDatabase() As TutorialDbContext
        Dim dbConfig = New DbConfig(Of TutorialDbContext)()
        Return dbConfig.OpenDirectory(New System.IO.DirectoryInfo("tutorial_db"))
    End Function

    ' -----------------------------------------------
    ' 3. 各種データベース処理 (CR_D)

    ' まずはレコードの挿入です。

    <TestMethod>
    Public Sub InsertSample()
        ' 先ほどの関数を用いてデータベースを作成します。
        Dim db As TutorialDbContext = CreateDatabase()

        ' 作成したばかりの Person テーブルには、レコードが 0 件含まれています。
        Assert.AreEqual(0L, db.Persons.CountAllRecords)

        ' 挿入するデータ (2016年7月現在)
        Dim person = New Person() With {.Name = "Hoshino, Gen", .Age = 35L}

        ' Insert メソッドを用いて、レコードの挿入を行います。
        db.Persons.Insert(person)

        ' 結果、Person テーブルのレコード数が 1 件になります。
        Assert.AreEqual(0L, person.Id)
        Assert.AreEqual(1L, db.Persons.CountAllRecords)
    End Sub

    ' 以降のサンプルからノイズを取り除くため、
    ' サンプルデータを含むデータベースを生成する関数を定義しておきます。

    Public Function CreateSampleDatabase() As TutorialDbContext
        Dim db = CreateDatabase()
        db.Persons.Insert(New Person() With {.Name = "Hoshino, Gen", .Age = 35L})
        db.Persons.Insert(New Person() With {.Name = "Ieiri, Leo", .Age = 21L})
        db.Songs.Insert(New Song() With {
                        .Title = "SUN",
                        .VocalName = New Lazy(Of String)(Function() "Hoshino, Gen")
                        })
        db.Songs.Insert(New Song() With {
                        .Title = "Hello To The World",
                        .VocalName = New Lazy(Of String)(Function() "Ieiri, Leo")
                        })
        Return db
    End Function

    ' 次に、レコードの取得を行います。

    <TestMethod>
    Public Sub ItemsSample()
        Dim db As TutorialDbContext = CreateSampleDatabase()

        ' Items プロパティは、すべての(削除されていない)レコードを返します。

        Dim items As IEnumerable(Of Person) = db.Persons.Items

        ' IEnumerable なので、クエリー式を用いて複雑なクエリーを書くこともできます。

        Dim queryResult =
            From person In db.Persons.Items
            Join song In db.Songs.Items On person.Name Equals song.VocalName.Value
            Where person.Age <= 22L
            Select New With {.Name = person.Name, .Title = song.Title, .Age = person.Age}

        Assert.AreEqual(1, queryResult.Count())
        Assert.AreEqual("Ieiri, Leo", queryResult.First().Name)
    End Sub

    <TestMethod>
    Public Sub ItemSample()
        Dim db As TutorialDbContext = CreateSampleDatabase()

        ' Item プロパティは、指定されたレコードIDを持つレコードを取得します。
        ' (テーブルはレコードの配列なので、この操作は定数時間で行えます。)

        Dim person = db.Persons.Item(0L)
        Assert.AreEqual("Hoshino, Gen", person.Name)
    End Sub

    ' 最後に、レコードの削除です。
    ' 常磐DBに「物理削除」は存在しないため、以下の Remove メソッドは「論理削除」を行います。

    <TestMethod>
    Public Sub RemoveSample()
        Dim db As TutorialDbContext = CreateSampleDatabase()

        ' 後で「削除を行う前の時点」を参照するために、
        ' 現在 (削除を行う前) の「リビジョン番号」を覚えておきます。
        Dim savedRevisionId = db.Database.CurrentRevisionId

        ' Remove メソッドは、指定したレコードIDを持つレコードを論理削除します。
        Dim hoshinoGen As Person = db.Persons.Items.First()
        Assert.AreEqual("Hoshino, Gen", hoshinoGen.Name)
        db.Persons.Remove(hoshinoGen.Id)

        ' 結果として、Person テーブルには
        ' Name = "Hoshino, Gen" となるレコードが存在しなくなります。
        Assert.IsFalse(db.Persons.Items.Any(Function(p) p.Name = hoshinoGen.Name))

        ' AllItems プロパティは、削除されたデータも含めて、すべてのレコードを取得します。
        Dim allPersons = db.Persons.AllItems

        ' 削除を行う前の時点 (リビジョン番号 savedRevisionId の時点) では、
        ' Name = "Hoshino, Gen" であるレコードが存在したことが分かります。
        Dim persons = db.Persons.AllItems.Where(Function(p) p.IsLiveAt(savedRevisionId))
        Assert.IsTrue(persons.Any(Function(p) p.Name = hoshinoGen.Name))
    End Sub
End Class
