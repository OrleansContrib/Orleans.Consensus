namespace Orleans.Consensus.UnitTests
{
    using FluentAssertions;
    using Log;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Xunit;

    public class BTreeTests
    {
        int[] testKeyData = new int[] { 10, 20, 30, 50 };
        int[] testPointerData = new int[] { 50, 60, 40, 20 };

        [Fact]
        public void BTreeIsCreatedEmpty()
        {
            var btree = new BTree<int, int>();
            var root = btree.Root;
            root.Should().NotBeNull();
            root.Entries.Should().NotBeNull();
            root.Children.Should().NotBeNull();
            root.Entries.Should().HaveCount(0);
            root.Children.Should().HaveCount(0);
        }

        [Fact]
        public void OneNodeIsInserted()
        {
            var btree = new BTree<int, int>();
            this.InsertTestDataAndValidateTree(btree, 0);
            btree.Height.Should().Be(1);
        }

        [Fact]
        public void MultipleNodesFormASplit()
        {
            var btree = new BTree<int, int>();

            for (int i = 0; i < this.testKeyData.Length; i++)
            {
                this.InsertTestDataAndValidateTree(btree, i);
            }

            btree.Height.Should().Be(2);
        }

        [Fact]
        public void NodesCanBeDeleted()
        {
            var btree = new BTree<int, int>();

            for (int i = 0; i < this.testKeyData.Length; i++)
            {
                this.InsertTestData(btree, i);
            }

            for (int i = 0; i < this.testKeyData.Length; i++)
            {
                btree.Delete(this.testKeyData[i]);
                ValidateTree(btree.Root, 2, this.testKeyData.Skip(i + 1).ToArray());
            }

            btree.Height.Should().Be(1);
        }

        [Fact]
        public void NodesCanBeDeletedInReverse()
        {
            var btree = new BTree<int, int>();

            for (int i = 0; i < this.testKeyData.Length; i++)
            {
                this.InsertTestData(btree, i);
            }

            for (int i = this.testKeyData.Length - 1; i > 0; i--)
            {
                btree.Delete(this.testKeyData[i]);
                ValidateTree(btree.Root, 2, this.testKeyData.Take(i).ToArray());
            }

            btree.Height.Should().Be(1);
        }

        [Fact]
        public void NonExistingNodesCanBeDeleted()
        {
            var btree = new BTree<int, int>();

            for (int i = 0; i < this.testKeyData.Length; i++)
            {
                this.InsertTestData(btree, i);
            }

            btree.Delete(99999);

            ValidateTree(btree.Root, 2, this.testKeyData);
        }

        [Fact]
        public void NodesCanBeSearched()
        {
            var btree = new BTree<int, int>();

            for (int i = 0; i < this.testKeyData.Length; i++)
            {
                this.InsertTestData(btree, i);
                this.SearchTestData(btree, i);
            }
        }

        [Fact]
        public void NonExistingNodesCanBeSearched()
        {
            var btree = new BTree<int, int>();

            // search an empty tree
            var nonExisting = btree.Search(9999);
            nonExisting.Should().BeNull();

            for (int i = 0; i < this.testKeyData.Length; i++)
            {
                this.InsertTestData(btree, i);
                this.SearchTestData(btree, i);
            }

            // search a populated tree
            nonExisting = btree.Search(9999);
            nonExisting.Should().BeNull();
        }

        [Fact]
        public void NodesAtExtremesCanBeRemoved()
        {
            var btree = new BTree<int, int>();
            var leftNode = new Node<int, int>();
            var rightNode = new Node<int, int>();

            btree.Root.Children.Add(leftNode);
            btree.Root.Children.Add(rightNode);

            leftNode.Entries.Add(new Entry<int, int>() { Key = 1, Pointer = 1 });
            leftNode.Entries.Add(new Entry<int, int>() { Key = 2, Pointer = 2 });
            btree.Root.Entries.Add(new Entry<int, int>() { Key = 3, Pointer = 3 });
            rightNode.Entries.Add(new Entry<int, int>() { Key = 4, Pointer = 4 });
            btree.Delete(4);
        }

        [Fact]
        public void HandlesLargeNumberOfNodes()
        {
            for (int i = 0; i < 100; i++)
            {
                RunBruteForce();
            }
        }

        void RunBruteForce()
        {
            var degree = 2;

            var btree = new BTree<string, int>(degree);

            var rand = new Random();
            for (int i = 0; i < 1000; i++)
            {
                var value = (int)rand.Next() % 100;
                var key = value.ToString();

                if (rand.Next() % 2 == 0)
                {
                    if (btree.Search(key) == null)
                    {
                        btree.Insert(key, value);
                    }
                    btree.Search(key).Pointer.Should().Be(value);
                }
                else {
                    btree.Delete(key);
                    btree.Search(key).Should().BeNull();
                }
                CheckNode(btree.Root, degree);
            }
        }

        void CheckNode(Node<string, int> node, int degree)
        {

            (node.Children.Count > 0 && node.Children.Count != node.Entries.Count + 1)
                .Should()
                .BeFalse("There are children, but they don't match the number of entries.");

            (node.Entries.Count > (2 * degree) - 1)
                .Should()
                .BeFalse("Too many entries in a node");
            

            (node.Children.Count > degree * 2)
                .Should()
                .BeFalse("Too much children in node");

            foreach (var child in node.Children)
            {
                CheckNode(child, degree);
            }
        }

        void InsertTestData(BTree<int, int> btree, int testDataIndex)
        {
            btree.Insert(this.testKeyData[testDataIndex], this.testPointerData[testDataIndex]);
        }

        void InsertTestDataAndValidateTree(BTree<int, int> btree, int testDataIndex)
        {
            btree.Insert(this.testKeyData[testDataIndex], this.testPointerData[testDataIndex]);
            ValidateTree(btree.Root, 2, this.testKeyData.Take(testDataIndex + 1).ToArray());
        }

        void SearchTestData(BTree<int, int> btree, int testKeyDataIndex)
        {
            for (int i = 0; i <= testKeyDataIndex; i++)
            {
                Entry<int, int> entry = btree.Search(this.testKeyData[i]);
                entry.Key.Should().Be(this.testKeyData[i]);
                entry.Pointer.Should().Be(this.testPointerData[i]);
            }
        }

        static void ValidateTree(Node<int, int> tree, int degree, params int[] expectedKeys)
        {
            var foundKeys = new Dictionary<int, List<Entry<int, int>>>();
            ValidateSubtree(tree, tree, degree, int.MinValue, int.MaxValue, foundKeys);

            expectedKeys.Except(foundKeys.Keys).Should().HaveCount(0);
            foreach (var keyValuePair in foundKeys)
            {
                keyValuePair.Value.Should().HaveCount(1);
            }
        }

        static void UpdateFoundKeys(Dictionary<int, List<Entry<int, int>>> foundKeys, Entry<int, int> entry)
        {
            List<Entry<int, int>> foundEntries;
            if (!foundKeys.TryGetValue(entry.Key, out foundEntries))
            {
                foundEntries = new List<Entry<int, int>>();
                foundKeys.Add(entry.Key, foundEntries);
            }

            foundEntries.Add(entry);
        }

        static void ValidateSubtree(Node<int, int> root, Node<int, int> node, int degree, int nodeMin, int nodeMax, Dictionary<int, List<Entry<int, int>>> foundKeys)
        {
            if (root != node)
            {
                (node.Entries.Count >= degree - 1).Should().BeTrue();
                (node.Entries.Count <= (2 * degree) - 1).Should().BeTrue();
            }

            for (int i = 0; i <= node.Entries.Count; i++)
            {
                int subtreeMin = nodeMin;
                int subtreeMax = nodeMax;

                if (i < node.Entries.Count)
                {
                    var entry = node.Entries[i];
                    UpdateFoundKeys(foundKeys, entry);
                    (entry.Key >= nodeMin && entry.Key <= nodeMax).Should().BeTrue();

                    subtreeMax = entry.Key;
                }

                if (i > 0)
                {
                    subtreeMin = node.Entries[i - 1].Key;
                }

                if (!node.IsLeaf)
                {
                    (node.Children.Count >= degree).Should().BeTrue();
                    ValidateSubtree(root, node.Children[i], degree, subtreeMin, subtreeMax, foundKeys);
                }
            }
        }

    }
}
