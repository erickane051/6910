using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApproximateAggregation
{
    class Program
    {
        static void Main(string[] args)
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();

            //Length and Width of the landscape
            int length = 500;
            int width = 500;

            //Range of possible sensed values
            int minSensedVal = 0;
            int maxSensedVal = 100;

            //Range of communication between nodes
            int communicationRange = 25;

            //Aggregate that we are looking for
            double percentileToCalc = 0.5;

            double epsilon = 0.5;
            int numberNodes = 500;
            int B = (int)Math.Ceiling((Math.Log(numberNodes, 2)) / epsilon);

            //Initialize base station at the middle of the grid
            Node baseStation = new Node();
            baseStation.xCoordinate = 0;
            baseStation.yCoordinate = 0;
            baseStation.sensedValues = new List<int>();
            baseStation.sensedValues.Add(numberNodes + 1);
            baseStation.ring = 0;
            baseStation.parent = null;
            baseStation.children = new List<Node>();
            baseStation.distanceToBase = 0;
            baseStation.localAggregate = new List<QuantileSummary>();

            //Initialize 1000 random nodes around the landscape
            List<Node> listNodes = new List<Node>();
            for(int i = 0; i < numberNodes; i++)
            {
                Node node = new Node();
                node.xCoordinate = RandomNumberBetween(-1 * (length / 2), (length / 2) + 1);
                node.yCoordinate = RandomNumberBetween(-1 * (width / 2), (width / 2) + 1);
                node.sensedValues = new List<int>();
                node.sensedValues.Add(i);
                node.distanceToBase = Math.Sqrt(Math.Pow(node.xCoordinate, 2) + Math.Pow(node.yCoordinate, 2));
                node.ring = null;
                node.id = i;
                node.children = new List<Node>();
                node.localAggregate = new List<QuantileSummary>();

                listNodes.Add(node);                
            }

            // Epoch 1 - keep track of round when each node is detected - All Broadcasting done from Base Station Version
            int numberRounds = length / communicationRange;
            for(int j = 1; j <= numberRounds; j++)
            {
                int minXCoord = j * -1 * (communicationRange / 2);
                int maxXCoord = j * (communicationRange / 2);
                int minYCoord = j * -1 * (communicationRange / 2);
                int maxYCoord = j * (communicationRange / 2);
            
                foreach (Node node in listNodes)
                {
                    if(node.xCoordinate >= minXCoord && node.xCoordinate <= maxXCoord && node.yCoordinate >= minYCoord && node.yCoordinate <= maxYCoord && node.ring == null)
                    {
                        node.ring = j;
                    }
                }
            }

            // Epoch 2 - each node chooses their parent
            double maxAcceptableRange = Math.Sqrt(Math.Pow(communicationRange / 2, 2) + Math.Pow(communicationRange / 2, 2));
            for(int k = numberRounds; k > 0; k--)
            {
                foreach (Node node in listNodes)
                {
                    if (node.ring == k && k != 1)
                    {
                        double minDistanceToBase = 9999;
                        foreach (Node compareNode in listNodes)
                        {
                            double compareDistance = Math.Sqrt(Math.Pow(node.xCoordinate - compareNode.xCoordinate, 2) + Math.Pow(node.yCoordinate - compareNode.yCoordinate, 2));

                            if(compareDistance <= maxAcceptableRange && compareNode.distanceToBase < minDistanceToBase && node.id != compareNode.id && node.ring == compareNode.ring + 1)
                            {
                                minDistanceToBase = compareNode.distanceToBase;
                                node.parent = compareNode;                              
                            }
                        }

                        if(node.parent != null)
                        {
                            int parentNodeId = node.parent.id.GetValueOrDefault();
                            listNodes[parentNodeId].children.Add(node);
                        }
                    }
                    else if(node.ring == k && k == 1)
                    {
                        node.parent = baseStation;
                        baseStation.children.Add(node);
                    }
                }
            }

            // Epoch 3 - BS broadcasts the percentile to calculate
            for (int l = 1; l <= numberRounds; l++)
            {
                foreach (Node node in listNodes)
                {
                    if (node.ring == l)
                    {
                        node.percentile = percentileToCalc;
                    }
                }
            }

            // Epoch 4 - Calculate percentile
            for (int m = numberRounds; m > 0; m--)
            {
                foreach (Node node in listNodes)
                {
                    if (node.ring == m)
                    {
                        //Sort the local values of the node and create the local aggregate summary                        
                        QuantileSummary quantileSummary = new QuantileSummary();
                        quantileSummary.summary = new List<SummaryTuple>();

                        node.sensedValues.Sort();
                        quantileSummary.size = node.sensedValues.Count();

                        for (int n = 0; n < node.sensedValues.Count(); n++)
                        {
                            SummaryTuple tuple = new SummaryTuple();
                            tuple.value = node.sensedValues[n];
                            tuple.rMin = n + 1;
                            tuple.rMax = n + 1;
                            quantileSummary.summary.Add(tuple);
                        }
                      
                        node.localAggregate.Add(quantileSummary);

                        //If node has children
                        if(node.children.Count > 0)
                        {
                            //For each child, add their quantile summaries to node's quantile summary list
                            foreach(Node childNode in node.children)
                            {                            
                                node.localAggregate.AddRange(childNode.localAggregate);
                            }

                            //Look for 2 summaries of similar size and merge
                            int begin = 0;
                            int end = 1;

                            while(end < node.localAggregate.Count())
                            {
                                int beginClass = (int)Math.Floor(Math.Log(node.localAggregate[begin].size, 2));
                                int endClass = (int)Math.Floor(Math.Log(node.localAggregate[end].size, 2));

                                if(beginClass == endClass)
                                {
                                    QuantileSummary summaryA = node.localAggregate[begin];
                                    QuantileSummary summaryB = node.localAggregate[end];
                                    node.localAggregate.RemoveAt(end);
                                    node.localAggregate.RemoveAt(begin);
                                    QuantileSummary mergedSummary = MergeSummaries(summaryA, summaryB, B);
                                    node.localAggregate.Insert(begin, mergedSummary);
                                    begin = 0;
                                    end = 1;
                                }
                                else
                                {
                                    if(end < node.localAggregate.Count() - 1)
                                    {
                                        end++;
                                    }
                                    else
                                    {
                                        begin++;
                                        end = begin + 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            //Merge at base station
            QuantileSummary baseSummary = new QuantileSummary();
            baseSummary.summary = new List<SummaryTuple>();

            baseStation.sensedValues.Sort();
            baseSummary.size = baseStation.sensedValues.Count();

            for (int n = 0; n < baseStation.sensedValues.Count(); n++)
            {
                SummaryTuple tuple = new SummaryTuple();
                tuple.value = baseStation.sensedValues[n];
                tuple.rMin = n + 1;
                tuple.rMax = n + 1;
                baseSummary.summary.Add(tuple);
            }

            baseStation.localAggregate.Add(baseSummary);

            //If base station has children
            if (baseStation.children.Count > 0)
            {
                //For each child, add their quantile summaries to base station's quantile summary list
                foreach (Node childNode in baseStation.children)
                {
                    baseStation.localAggregate.AddRange(childNode.localAggregate);
                }

                while (baseStation.localAggregate.Count() > 1)
                {
                    QuantileSummary summaryA = baseStation.localAggregate[0];
                    QuantileSummary summaryB = baseStation.localAggregate[1];
                    baseStation.localAggregate.RemoveAt(1);
                    baseStation.localAggregate.RemoveAt(0);
                    QuantileSummary mergedSummary = MergeSummaries(summaryA, summaryB, B);
                    baseStation.localAggregate.Insert(0, mergedSummary);
                }
            }

            // the code that you want to measure comes here
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            var y = "y";
        }

        private static readonly Random random = new Random();

        private static int RandomNumberBetween(int minValue, int maxValue)
        {
            return random.Next(minValue, maxValue);
        }

        public static QuantileSummary MergeSummaries(QuantileSummary X, QuantileSummary Y, int B)
        {
            List<SummaryTuple> Q = new List<SummaryTuple>();
            QuantileSummary returnSummary = new QuantileSummary();

            int indexX = 0;
            int indexY = 0;

            for (int o = 0; o < X.summary.Count() + Y.summary.Count; o++)
            {
                SummaryTuple newTuple = new SummaryTuple();

                if (indexY < Y.summary.Count() && indexX < X.summary.Count())
                {
                    if(X.summary[indexX].value < Y.summary[indexY].value)
                    {
                        SummaryTuple Ys = FindLargestLessThanValue(X.summary[indexX].value, Y.summary);
                        SummaryTuple Yt = FindSmallestGreaterThanValue(X.summary[indexX].value, Y.summary);

                        newTuple.value = X.summary[indexX].value;

                        if (Ys == null)
                        {
                            newTuple.rMin = X.summary[indexX].rMin;
                        }
                        else
                        {
                            newTuple.rMin = X.summary[indexX].rMin + Ys.rMin;
                        }

                        if (Yt == null)
                        {
                            newTuple.rMax = X.summary[indexX].rMax + Ys.rMax;
                        }
                        else
                        {
                            newTuple.rMax = X.summary[indexX].rMax + Yt.rMax - 1;
                        }

                        indexX++;
                    }
                    else
                    {
                        SummaryTuple Xs = FindLargestLessThanValue(Y.summary[indexY].value, X.summary);
                        SummaryTuple Xt = FindSmallestGreaterThanValue(Y.summary[indexY].value, X.summary);

                        newTuple.value = Y.summary[indexY].value;

                        if (Xs == null)
                        {
                            newTuple.rMin = Y.summary[indexY].rMin;
                        }
                        else
                        {
                            newTuple.rMin = Y.summary[indexY].rMin + Xs.rMin;
                        }

                        if (Xt == null)
                        {
                            newTuple.rMax = Y.summary[indexY].rMax + Xs.rMax;
                        }
                        else
                        {
                            newTuple.rMax = Y.summary[indexY].rMax + Xt.rMax - 1;
                        }

                        indexY++;
                    }
                }
                else
                {
                    if (indexY > Y.summary.Count() - 1)
                    {
                        SummaryTuple Ys = FindLargestLessThanValue(X.summary[indexX].value, Y.summary);
                        SummaryTuple Yt = FindSmallestGreaterThanValue(X.summary[indexX].value, Y.summary);

                        newTuple.value = X.summary[indexX].value;

                        if (Ys == null)
                        {
                            newTuple.rMin = X.summary[indexX].rMin;
                        }
                        else
                        {
                            newTuple.rMin = X.summary[indexX].rMin + Ys.rMin;
                        }

                        if (Yt == null)
                        {
                            newTuple.rMax = X.summary[indexX].rMax + Ys.rMax;
                        }
                        else
                        {
                            newTuple.rMax = X.summary[indexX].rMax + Yt.rMax - 1;
                        }

                        indexX++;
                    }
                    else
                    {
                        SummaryTuple Xs = FindLargestLessThanValue(Y.summary[indexY].value, X.summary);
                        SummaryTuple Xt = FindSmallestGreaterThanValue(Y.summary[indexY].value, X.summary);

                        newTuple.value = Y.summary[indexY].value;

                        if (Xs == null)
                        {
                            newTuple.rMin = Y.summary[indexY].rMin;
                        }
                        else
                        {
                            newTuple.rMin = Y.summary[indexY].rMin + Xs.rMin;
                        }

                        if (Xt == null)
                        {
                            newTuple.rMax = Y.summary[indexY].rMax + Xs.rMax;
                        }
                        else
                        {
                            newTuple.rMax = Y.summary[indexY].rMax + Xt.rMax - 1;
                        }

                        indexY++;
                    }
                }


                Q.Add(newTuple);
            }

            returnSummary.summary = Q;
            returnSummary.size = X.size + Y.size;

            if(returnSummary.size > B)
            {
                returnSummary = Prune(returnSummary, B);
            }

            return returnSummary;
        }

        public static SummaryTuple FindLargestLessThanValue(int value, List<SummaryTuple> list)
        {
            int? placeholder = null;

            for(int i = 0; i < list.Count(); i++)
            {
                if(list[i].value < value)
                {
                    placeholder = i;
                }
            }

            if(placeholder == null)
            {
                return null;
            }
            else
            {
                return list[placeholder.GetValueOrDefault()];
            }
        }

        public static SummaryTuple FindSmallestGreaterThanValue(int value, List<SummaryTuple> list)
        {
            int? placeholder = null;

            for (int i = list.Count() - 1; i >= 0; i--)
            {
                if (list[i].value >= value)
                {
                    placeholder = i;
                }
            }

            if (placeholder == null)
            {
                return null;
            }
            else
            {
                return list[placeholder.GetValueOrDefault()];
            }
        }

        public static QuantileSummary Prune(QuantileSummary quantileSummary, int B)
        {
            List<int> entriesToKeep = new List<int>();
            List<SummaryTuple> replacementSummary = new List<SummaryTuple>();

            entriesToKeep.Add(0);

            int i = 1;
            int x = 0;

            while(x < quantileSummary.summary.Count())
            {
                x = (int)Math.Floor((quantileSummary.summary.Count() * i) / (decimal)B) - 1;

                if(x < quantileSummary.summary.Count() - 1)
                {
                    entriesToKeep.Add(x);
                }
                
                i++;
            }

            entriesToKeep.Add(quantileSummary.summary.Count() - 1);

            for(int j = 0; j < entriesToKeep.Count(); j++)
            {
                replacementSummary.Add(quantileSummary.summary[entriesToKeep[j]]);
            }

            quantileSummary.summary = replacementSummary;

            return quantileSummary;

        }

    }
}
