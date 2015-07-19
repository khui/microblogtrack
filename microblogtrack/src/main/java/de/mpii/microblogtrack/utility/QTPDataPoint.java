package de.mpii.microblogtrack.utility;

import ciir.umass.edu.learning.DataPoint;
import gnu.trove.map.TObjectDoubleMap;

/**
 * wrapper to convert to DataPoint used in ranklib
 *
 * @author khui
 */
public class QTPDataPoint extends DataPoint {

    public QTPDataPoint(QueryTweetPair qtp) {
        super();
        TObjectDoubleMap<String> featureValues = qtp.getFeatures();
        String[] featureNames = QueryTweetPair.getFeatureNames();
        float fval = 0;
        fVals = new float[featureNames.length + 1];
        for (int i = 1; i <= featureNames.length; i++) {
            if (featureValues.containsKey(featureNames[i - 1])) {
                fval = (float) featureValues.get(featureNames[i - 1]);
            }
            fVals[i] = fval;
        }
        featureCount = fVals.length;
        label = 0;
        id = String.valueOf(qtp.tweetid);
        description = "";
        knownFeatures = featureCount;
    }


    @Override
    public float getFeatureValue(int i) {
        return fVals[i];
    }

    @Override
    public void setFeatureValue(int i, float f) {
        fVals[i] = f;
    }

    @Override
    public void setFeatureVector(float[] floats) {
        fVals = floats;
    }

    @Override
    public float[] getFeatureVector() {
        return fVals;
    }

}
