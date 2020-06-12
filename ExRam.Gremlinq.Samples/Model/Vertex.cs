﻿using ExRam.Gremlinq.Core.GraphElements;

namespace ExRam.Gremlinq.Samples
{
    public class Vertex :  IVertex
    {
        public object Id { get; set; }
        public string PartitionKey { get; set; } = "PartitionKey";
    }
}
