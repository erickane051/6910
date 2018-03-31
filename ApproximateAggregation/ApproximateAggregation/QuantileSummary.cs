using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApproximateAggregation
{
    public class QuantileSummary
    {
        public List<SummaryTuple> summary { get; set; }
        public int size { get; set; }
    }
}
