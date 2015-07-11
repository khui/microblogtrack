package de.mpii.microblogtrack.component.thirdparty;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;

/**
 * Implements Rocchio's pseudo feedback QueryExpansion algorithm
 * <p>
 * Query Expansion - Adding search terms to a user's search. Query expansion is
 * the process of a search engine adding search terms to a user's weighted
 * search. The intent is to improve precision and/or recall. The additional
 * terms may be taken from a thesaurus. For example a search for "car" may be
 * expanded to: car cars auto autos automobile automobiles [foldoc.org].
 *
 * To see options that could be configured through the properties file @see
 * Constants Section
 * <p>
 * Created on February 23, 2005, 5:29 AM
 * <p>
 * TODO: Yahoo started providing API to query www; could be nice to add yahoo
 * implementation as well
 * <p>
 * @author Neil O. Rouben
 *
 * Modified LucQE a little to make it get along with lucene 5.2, meanwhile
 * adjust the way of passing parameters
 * @author khui
 */
public class QueryExpansion {

    static Logger logger = Logger.getLogger(QueryExpansion.class.getName());

    private final Analyzer analyzer;
    private final IndexSearcher searcher;
    private List<TermQuery> expandedTerms;
    private int termNum = 20, docNum = 10;
    private float alpha = 0.8f, beta = 0.4f, decay = 0f;
    private final String fieldname;

    /**
     * Creates a new instance of QueryExpansion
     *
     * @param analyzer - used to parse documents to extract terms
     * @param searcher - used to obtain idf
     * @param fieldname
     */
    public QueryExpansion(Analyzer analyzer, IndexSearcher searcher, String fieldname) {
        this.analyzer = analyzer;
        this.searcher = searcher;
        this.fieldname = fieldname;
    }

    public void setParameters(int termNum, int docNum, float alpha, float beta, float decay) {
        this.termNum = termNum;
        this.docNum = docNum;
        this.alpha = alpha;
        this.beta = beta;
        this.decay = decay;
    }

    /**
     * Performs Rocchio's query expansion with pseudo feedback qm = alpha *
     * query + ( beta / relevanDocsCount ) * Sum ( rel docs vector )
     *
     * @param queryStr - that will be expanded
     * @param hits - from the original query to use for expansion
     *
     * @return expandedQuery
     *
     * @throws IOException
     * @throws ParseException
     */
    public Query expandQuery(String queryStr, ScoreDoc[] hits)
            throws IOException, ParseException {
        // Get Docs to be used in query expansion
        List<Document> vHits = getDocs(hits);

        return expandQuery(queryStr, vHits);
    }

    /**
     * Gets documents that will be used in query expansion. number of docs
     * indicated by <code>QueryExpansion.DOC_NUM_FLD</code> from <code> QueryExpansion.DOC_SOURCE_FLD
     * </code>
     *
     * @param query - for which expansion is being performed
     * @param hits - to use in case <code> QueryExpansion.DOC_SOURCE_FLD </code>
     * is not specified
     * @param prop - uses <code> QueryExpansion.DOC_SOURCE_FLD </code> to
     * determine where to get docs
     *
     * @return number of docs indicated by
     * <code>QueryExpansion.DOC_NUM_FLD</code> from <code> QueryExpansion.DOC_SOURCE_FLD
     * </code>
     * @throws IOException
     * @throws GoogleSearchFault
     */
    private List<Document> getDocs(ScoreDoc[] hits) throws IOException {
        List<Document> vHits = new ArrayList<>();

        for (int i = 0; ((i < docNum) && (i < hits.length)); i++) {
            vHits.add(searcher.doc(hits[i].doc));
        }
        return vHits;
    }

    /**
     * Performs Rocchio's query expansion with pseudo feedback qm = alpha *
     * query + ( beta / relevanDocsCount ) * Sum ( rel docs vector )
     *
     * @param queryStr - that will be expanded
     * @param hits - from the original query to use for expansion
     *
     * @return
     * @throws IOException
     * @throws ParseException
     */
    public Query expandQuery(String queryStr, List<Document> hits)
            throws IOException, ParseException {

        // Create combine documents term vectors - sum ( rel term vectors )
        List<QueryTermVector> docsTermVector = getDocsTerms(hits, docNum, analyzer);

        // Adjust term features of the docs with alpha * query; and beta; and assign weights/boost to terms (tf*idf)
        Query expandedQuery = adjust(docsTermVector, queryStr, alpha, beta, decay, docNum, termNum);

        return expandedQuery;
    }

    /**
     * Adjust term features of the docs with alpha * query; and beta; and assign
     * weights/boost to terms (tf*idf).
     *
     * @param docsTermsVector of the terms of the top
     *        <code> docsRelevantCount </code> documents returned by original query
     * @param queryStr - that will be expanded
     * @param alpha - factor of the equation
     * @param beta - factor of the equation
     * @param decay
     * @param docsRelevantCount - number of the top documents to assume to be
     * relevant
     * @param maxExpandedQueryTerms - maximum number of terms in expanded query
     *
     * @return expandedQuery with boost factors adjusted using Rocchio's
     * algorithm
     *
     * @throws IOException
     * @throws ParseException
     */
    private Query adjust(List<QueryTermVector> docsTermsVector, String queryStr,
            float alpha, float beta, float decay, int docsRelevantCount,
            int maxExpandedQueryTerms)
            throws IOException, ParseException {
        Query expandedQuery;

        // setBoost of docs terms
        List<TermQuery> docsTerms = setBoost(docsTermsVector, beta, decay);
        logger.info(docsTerms.toString());

        // setBoost of query terms
        // Get queryTerms from the query
        QueryTermVector queryTermsVector = new QueryTermVector(queryStr, analyzer);
        List<TermQuery> queryTerms = setBoost(queryTermsVector, alpha);

        // combine weights according to expansion formula
        List<TermQuery> expandedQueryTerms = combine(queryTerms, docsTerms);
        setExpandedTerms(expandedQueryTerms);
        // Sort by boost=weight, in descending order
        Collections.sort(expandedQueryTerms, (TermQuery q1, TermQuery q2) -> {
            if (q1.getBoost() > q2.getBoost()) {
                return -1;
            } else {
                return 1;
            }
        });

        // Create Expanded Query
        expandedQuery = mergeQueries(expandedQueryTerms, maxExpandedQueryTerms);
        logger.info(expandedQuery.toString());

        return expandedQuery;
    }

    /**
     * Merges <code>termQueries</code> into a single query. In the future this
     * method should probably be in <code>Query</code> class. This is akward way
     * of doing it; but only merge queries method that is available is
     * mergeBooleanQueries; so actually have to make a string term1^boost1,
     * term2^boost and then parse it into a query
     *
     * @param termQueries - to merge
     * @param maxTerms
     *
     * @return query created from termQueries including boost parameters
     * @throws org.apache.lucene.queryparser.classic.ParseException
     */
    private Query mergeQueries(List<TermQuery> termQueries, int maxTerms)
            throws ParseException {
        Query query;

        // Select only the maxTerms number of terms
        int termCount = Math.min(termQueries.size(), maxTerms);

        // Create Query String
        StringBuilder qBuf = new StringBuilder();
        for (int i = 0; i < termCount; i++) {
            TermQuery termQuery = termQueries.get(i);
            Term term = termQuery.getTerm();
            qBuf.append(term.text()).append("^").append(termQuery.getBoost()).append(" ");
            logger.info(term + " : " + termQuery.getBoost());
        }

        // Parse StringQuery to create Query
        logger.info(qBuf.toString());
        //QueryParser qp = new QueryParser(qBuf.toString(), analyzer);
        QueryParser qp = new QueryParser(fieldname, analyzer);
        query = qp.parse(qBuf.toString());
        logger.info(query.toString());
        return query;
    }

    /**
     * Extracts terms of the documents; Adds them to vector in the same order
     *
     * @param hits
     * @param docsRelevantCount - number of the top documents to assume to be
     * relevant
     * @param analyzer - to extract terms
     *
     * @return docsTerms docs must be in order
     * @throws java.io.IOException
     */
    private List<QueryTermVector> getDocsTerms(List<Document> hits, int docsRelevantCount, Analyzer analyzer)
            throws IOException {
        List<QueryTermVector> docsTerms = new ArrayList<>();

        // Process each of the documents
        for (int i = 0; ((i < docsRelevantCount) && (i < hits.size())); i++) {
            Document doc = hits.get(i);
            // Get text of the document and append it
            StringBuilder docTxtBuffer = new StringBuilder();
            String[] docTxtFlds = doc.getValues(fieldname);
            for (String docTxtFld : docTxtFlds) {
                docTxtBuffer.append(docTxtFld).append(" ");
            }

            // Create termVector and add it to vector
            QueryTermVector docTerms = new QueryTermVector(docTxtBuffer.toString(), analyzer);
            docsTerms.add(docTerms);
        }

        return docsTerms;
    }

    /**
     * Sets boost of terms. boost = weight = factor(tf*idf)
     *
     * @param termVector
     * @param factor
     * @return
     * @throws java.io.IOException
     */
    private List<TermQuery> setBoost(QueryTermVector termVector, float factor)
            throws IOException {
        List<QueryTermVector> v = new ArrayList<>();
        v.add(termVector);

        return setBoost(v, factor, 0);
    }

    /**
     * Sets boost of terms. boost = weight = factor(tf*idf)
     *
     * @param docsTerms
     * @param factor - adjustment factor ( ex. alpha or beta )
     * @param decayFactor
     * @return
     * @throws java.io.IOException
     */
    private List<TermQuery> setBoost(List<QueryTermVector> docsTerms, float factor, float decayFactor)
            throws IOException {
        List<TermQuery> terms = new ArrayList<>();

        // setBoost for each of the terms of each of the docs
        for (int g = 0; g < docsTerms.size(); g++) {
            QueryTermVector docTerms = docsTerms.get(g);
            String[] termsTxt = docTerms.getTerms();
            int[] termFrequencies = docTerms.getTermFrequencies();

            // Increase docdecay
            float docdecay = decayFactor * g;

            // Populate terms: with TermQuries and set boost
            for (int i = 0; i < docTerms.size(); i++) {
                // Create Term
                String termTxt = termsTxt[i];
                Term term = new Term(fieldname, termTxt);

                // Calculate weight
                float tf = termFrequencies[i];
                float idf = idf(term);
                float weight = tf * idf;
                // Adjust weight by docdecay factor
                weight = weight - (weight * docdecay);
                logger.info("weight: " + weight);

                // Create TermQuery and add it to the collection
                TermQuery termQuery = new TermQuery(term);
                // Calculate and set boost
                termQuery.setBoost(factor * weight);
                terms.add(termQuery);
            }
        }

        // Get rid of duplicates by merging termQueries with equal terms
        merge(terms);

        return terms;
    }

    private float idf(Term term) throws IOException {
        int N = searcher.getIndexReader().numDocs();
        int df = searcher.getIndexReader().docFreq(term);
        if (df > 0) {
            float idf = (float) Math.log(N / (double) df);
            return idf;
        } else {
            logger.error(term.text() + ": df=" + df);
            return 1;
        }
    }

    /**
     * Gets rid of duplicates by merging termQueries with equal terms
     *
     * @param terms
     */
    private void merge(List<TermQuery> terms) {
        for (int i = 0; i < terms.size(); i++) {
            TermQuery term = terms.get(i);
            // Itterate through terms and if term is equal then merge: add the boost; and delete the term
            for (int j = i + 1; j < terms.size(); j++) {
                TermQuery tmpTerm = terms.get(j);

                // If equal then merge
                if (tmpTerm.getTerm().text().equals(term.getTerm().text())) {
                    // Add boost factors of terms
                    term.setBoost(term.getBoost() + tmpTerm.getBoost());
                    // delete uncessary term
                    terms.remove(j);
                    // decrement j so that term is not skipped
                    j--;
                }
            }
        }
    }

    /**
     * combine weights according to expansion formula
     *
     * @param queryTerms
     * @param docsTerms
     * @return
     */
    private List<TermQuery> combine(List<TermQuery> queryTerms, List<TermQuery> docsTerms) {
        List<TermQuery> terms = new ArrayList<>();
        // Add Terms from the docsTerms
        terms.addAll(docsTerms);
        // Add Terms from queryTerms: if term already exists just increment its boost
        for (int i = 0; i < queryTerms.size(); i++) {
            TermQuery qTerm = queryTerms.get(i);
            TermQuery term = find(qTerm, terms);
            // Term already exists update its boost
            if (term != null) {
                float weight = qTerm.getBoost() + term.getBoost();
                term.setBoost(weight);
            } // Term does not exist; add it
            else {
                terms.add(qTerm);
            }
        }

        return terms;
    }

    /**
     * Finds term that is equal
     *
     * @param term
     * @param terms
     * @return term; if not found -> null
     */
    private TermQuery find(TermQuery term, List<TermQuery> terms) {
        TermQuery termF = null;

        Iterator<TermQuery> iterator = terms.iterator();
        while (iterator.hasNext()) {
            TermQuery currentTerm = iterator.next();
            if (term.getTerm().equals(currentTerm.getTerm())) {
                termF = currentTerm;
                logger.info("Term Found: " + term);
            }
        }

        return termF;
    }

    /**
     * Returns <code> QueryExpansion.TERM_NUM_FLD </code> expanded terms from
     * the most recent query
     *
     * @return
     */
    private List<TermQuery> getExpandedTerms() {
        List<TermQuery> terms = new ArrayList<>();

        // Return only necessary number of terms
        List<TermQuery> list = this.expandedTerms.subList(0, termNum);
        terms.addAll(list);

        return terms;
    }

    private void setExpandedTerms(List<TermQuery> expandedTerms) {
        this.expandedTerms = expandedTerms;
    }

    /**
     * class originally being included in lucene, but removed now. Construct a
     * class with the methods used in this class, with the same name of the
     * original class
     */
    private class QueryTermVector {

        private final String[] terms;

        private final int[] tfs;

        QueryTermVector(String str, Analyzer analyzer) throws IOException {
            TObjectIntMap<String> result = new TObjectIntHashMap<>();
            TokenStream stream = analyzer.tokenStream("field name", str);
            CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            try {
                while (stream.incrementToken()) {
                    result.adjustOrPutValue(charTermAttribute.toString(), 1, 1);
                }
                stream.end();
                stream.close();
            } catch (IOException ex) {
                logger.error("", ex);
            }
            terms = result.keys(new String[0]);
            tfs = new int[terms.length];
            for (int i = 0; i < terms.length; i++) {
                tfs[i] = result.get(terms[i]);
            }
        }

        public String[] getTerms() {
            return terms;
        }

        public int[] getTermFrequencies() {
            return tfs;
        }

        public int size() {
            return terms.length;
        }

    }

}
