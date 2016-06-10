using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using TokiwaDb.Core.Utilities;

namespace TokiwaDb.Core.Test.Utilities
{
    [TestFixture]
    public class PersistentBTreeTest
    {
        [Test]
        public void EmptyTest()
        {
            var bt = new PersistentBTree();
            Assert.AreEqual(0, bt.Count);
        }
    }
}
