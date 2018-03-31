using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApproximateAggregation
{
    public class Node
    {
        public int? id { get; set; }
        public int xCoordinate { get; set; }
        public int yCoordinate { get; set; }
        public List<int> sensedValues { get; set; }
        public int? ring { get; set; }
        public double distanceToBase { get; set; }
        public double percentile { get; set; }
        public Node parent { get; set; }
        public List<Node> children { get; set; }
        public List<QuantileSummary> localAggregate { get; set; }
    }
}
