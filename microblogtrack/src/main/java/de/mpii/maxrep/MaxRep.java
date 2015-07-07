package de.mpii.maxrep;

import de.mpii.microblogtrack.utility.Configuration;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TDoubleObjectMap;
import gnu.trove.map.hash.TDoubleObjectHashMap;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Centroid;

/**
 * java implementation of our maxrep algorithm. given n data points, we target
 * at selecting k points that can best represent the remaining data points in
 * terms of similarity among data points
 *
 * @author khui
 */
public class MaxRep {

    static Logger logger = Logger.getLogger(MaxRep.class.getName());

    private final Centroid[] datapoints;

    private final int n;

    private final double[][] similarityMatrix;

    private final double[] maxRepInSelectedsetVector;
    // the selected data point, representing by the index of the data in the input data points array
    private final TIntList selectedL = new TIntArrayList();

    private final DistanceMeasure distanceMeasure;

    private final double similarityThreshold = Configuration.MAXREP_SIMI_THRESHOLD;

    public MaxRep(Centroid[] datapoints) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.datapoints = datapoints;
        this.distanceMeasure = (DistanceMeasure) Class.forName(Configuration.MAXREP_DISTANT_MEASURE).newInstance();
        this.n = datapoints.length;
        this.maxRepInSelectedsetVector = new double[n];
        this.similarityMatrix = new double[n][];
        for (int i = 0; i < n; i++) {
            this.similarityMatrix[i] = new double[n];
        }
        initSimimatrix();
    }

    /**
     * main entrance of the method, pick up k data points as the representative
     * points in terms of weighted similarity. return the keys of the selected
     * centroids.
     *
     * @param k
     * @return
     */
    public int[] selectMaxRep(int k) {
        while (selectedL.size() <= Math.min(n, k)) {
            int max_simigain_index = getTopRep();
            selectedL.add(max_simigain_index);
            updateMaxRepVector(max_simigain_index);
        }
        // we use the key of the selected centroids as the final results
        TIntList selectedCentroidKeys = new TIntArrayList();
        for (int selectedIndex : selectedL.toArray()) {
            selectedCentroidKeys.add(datapoints[selectedIndex].getIndex());
        }
        return selectedCentroidKeys.toArray();
    }

    /**
     * in each round, we pick up the data point that can provide the maximum
     * similarity gain for the selected points set w.r.t. the complete data set.
     * for each data point i among all points, the comparison between its
     * weighted similarity against every other data points j, and j'th maximum
     * similarity w.r.t. all selected points in L, aggregating the positive gain
     * in delta_simi_sum. Afterward, pick up the data point i with max
     * delta_simi_sum to add to the selected set. the weight for each data point
     * indicates their importance. intuitively, we want to pick up the data
     * point into the selected set that can maximum increase the selected set's
     * weighted representativeness in terms of similarity.
     *
     * @return
     */
    private int getTopRep() {
        TDoubleObjectMap<TIntList> deltaSimSums_indexes = new TDoubleObjectHashMap<>();
        // go thru all data points as candidate data points to be selected
        for (int i = 0; i < n; i++) {
            double delta_simi_sum = 0;
            // for i-th data point, compare it with every other data points
            // to compute the possible similarity gain, aggregating in delta_simi_sum
            for (int j = 0; j < n; j++) {
                double ij_similarity = similarityMatrix[i][j] * datapoints[j].getWeight();
                double max_simi_j = maxRepInSelectedsetVector[j];
                delta_simi_sum += Math.max(ij_similarity - max_simi_j, 0);
            }
            if (!deltaSimSums_indexes.containsKey(delta_simi_sum)) {
                deltaSimSums_indexes.put(delta_simi_sum, new TIntArrayList());
            }
            deltaSimSums_indexes.get(delta_simi_sum).add(i);
        }
        double max_delta_simi = Collections.max(Arrays.asList(ArrayUtils.toObject(deltaSimSums_indexes.keys())));
        int selected_data_index = deltaSimSums_indexes.get(max_delta_simi).get(0);
        return selected_data_index;
    }

    /**
     * update the max-similarity array, representing the maximum similarity
     * among the selected data points and current data point. the similarity
     * being updated is the multiplication between similarity and the weight of
     * current data point.
     *
     * @param selected_data_index
     */
    private void updateMaxRepVector(int selected_data_index) {
        for (int i = 0; i < n; i++) {
            maxRepInSelectedsetVector[i] = Math.max(maxRepInSelectedsetVector[i],
                    similarityMatrix[i][selected_data_index] * datapoints[i].getWeight());
        }
    }

    /**
     * generate n X n similarity matrix for the input n data points, similarity
     * value should lay between 0 and 1, inclusive
     */
    private void initSimimatrix() {
        // compute similarity for upper diangel  
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                double distance = distanceMeasure.distance(datapoints[i], datapoints[j]);
                if (distance < 0 || distance > 1) {
                    logger.error("Illegal distance: " + distance);
                    distance = (distance < 0 ? 0 : 1);
                }
                // regard datapoints far away enough as non-similar
                if (distance > 1 - similarityThreshold) {
                    distance = 1;
                }
                this.similarityMatrix[i][j] = 1 - distance;
            }
        }
        // make the similarity matrix symmetrics in favor of further computation
        for (int i = 0; i < n; i++) {
            for (int j = 0; j <= i; j++) {
                if (i == j) {
                    this.similarityMatrix[i][j] = 1;
                } else {
                    this.similarityMatrix[i][j] = this.similarityMatrix[j][i];
                }
            }
        }
    }

}
