package com.clearspring.websnake;

import com.clearspring.codec.Codec;
import com.clearspring.data.bundle.Bundle;
import com.clearspring.data.bundle.BundleField;
import com.clearspring.data.filter.bundle.BundleFilter;
import com.clearspring.data.util.CombinatorialIterator;
import com.clearspring.data.value.ValueFactory;
import com.clearspring.data.value.ValueObject;
import com.clearspring.util.Debug;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class StopWordStripper extends StreamBuilder
{
    private final Debug log = Debug.getShortName(StreamComboBuilder.class);

    /**
     * An unmodifiable set containing some common English words that are not
     * usually useful for searching.
     */
    public static final Set<?> ENGLISH_STOP_WORDS_SET;

    static
    {
        final List<String> stopWords = Arrays.asList("a", "able", "about", "across", "after", "all", "almost", "also", "am", "among", "an", "and", "any", "are", "as", "at", "be", "because", "been",
                "but", "by", "can", "cannot", "could", "dear", "did", "do", "does", "either", "else", "ever", "every", "for", "from", "get", "got", "had", "has", "have", "he", "her", "hers", "him",
                "his", "how", "however", "i", "if", "in", "into", "is", "it", "its", "just", "least", "let", "like", "likely", "may", "me", "might", "most", "must", "my", "neither", "no", "nor",
                "not", "of", "off", "often", "on", "only", "or", "other", "our", "own", "rather", "said", "say", "says", "she", "should", "since", "so", "some", "than", "that", "the", "their",
                "them", "then", "there", "these", "they", "this", "tis", "to", "too", "twas", "us", "wants", "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why",
                "will", "with", "would", "yet", "you", "your");
        ENGLISH_STOP_WORDS_SET = new HashSet<String>(stopWords);
    }

    private final Pattern badDataPattern = Pattern.compile("\\W+");


    @Codec.Set(codable = true)
    private String comboField;
    @Codec.Set(codable = true)
    private String join = ",";
    @Codec.Set(codable = true)
    private int combo = 4;
    @Codec.Set(codable = true)
    private boolean dropOnNullToken = true;
    @Codec.Set(codable = true)
    private boolean sortTokens = true;
    @Codec.Set(codable = true)
    private String firstTokenField;
    @Codec.Set(codable = true)
    private boolean forceLowerCase = true;
    @Codec.Set(codable = true)
    private boolean dropStopWords = false;
    @Codec.Set(codable = true)
    private BundleFilter filter;

    public void setComboField(String comboField)
    {
        this.comboField = comboField;
    }

    public void setJoin(String join)
    {
        this.join = join;
    }

    public void setCombo(int combo)
    {
        this.combo = combo;
    }

    public void setDropOnNullToken(boolean dropOnNullToken)
    {
        this.dropOnNullToken = dropOnNullToken;
    }

    public void setSortTokens(boolean sortTokens)
    {
        this.sortTokens = sortTokens;
    }

    public void setFirstTokenField(String firstTokenField)
    {
        this.firstTokenField = firstTokenField;
    }

    public void setForceLowerCase(boolean forceLowerCase)
    {
        this.forceLowerCase = forceLowerCase;
    }

    public void setDropStopWords(boolean dropStopWords)
    {
        this.dropStopWords = dropStopWords;
    }

    @Override
    public void process(Bundle bundle, StreamEmitter emitter)
    {
        ValueObject valueObject = bundle.getValue(bundle.getFormat().getField(comboField));
        String comboValue = null;
        if (valueObject != null)
        {
            comboValue = valueObject.asString().toString();
        }
        if (log.ok(1))
        {
            log.emit("Value to create combinations for is: " + comboValue);
        }

        if ((comboValue == null || comboValue.isEmpty()) && !dropOnNullToken)
        {
            emitter.emit(bundle);
        }
        else if (comboValue != null)
        {
            List<List<String>> subTokenLists = getSubTokenLists(comboValue.split(","));
            Set<String> tokenChainSet = new HashSet<String>();
            for (List<String> subTokenList : subTokenLists)
            {
                for (int i = 1; i <= Math.min(subTokenList.size(), combo); i++)
                {
                    CombinatorialIterator<String> combinatorialIterator = new CombinatorialIterator<String>(i, subTokenList);
                    while (combinatorialIterator.hasNext())
                    {
                        List<String> next = combinatorialIterator.next();
                        if (sortTokens)
                        {
                            Collections.sort(next);
                        }
                        StringBuilder sb = new StringBuilder();
                        for (String token : next)
                        {
                            if (sb.length() > 0)
                            {
                                sb.append(join);
                            }
                            sb.append(token);
                        }
                        String tokenChain = sb.toString();
                        if (!tokenChainSet.contains(tokenChain))
                        {
                            tokenChainSet.add(tokenChain);
                            // create new line with substituted value
                            Bundle newBundle = bundle.createBundle();
                            for (BundleField bundleField : bundle.getFormat())
                            {
                                // TODO use index
                                if (bundleField.getName().equals(comboField))
                                {
                                    newBundle.setValue(bundleField, ValueFactory.create(tokenChain));
                                }
                                else
                                {
                                    newBundle.setValue(bundleField, bundle.getValue(bundleField));
                                }
                            }
                            if (firstTokenField != null && next.get(0) != null)
                            {
                                newBundle.setValue(newBundle.getFormat().getField(firstTokenField), ValueFactory.create(next.get(0)));
                            }
                            if (filter == null || filter.filter(newBundle))
                            {

                                emitter.emit(newBundle);
                            }
                        }
                    }
                }
            }
        }
    }

    private List<List<String>> getSubTokenLists(String[] tokenArray)
    {
        List<String> tokenList = Arrays.asList(tokenArray);
        List<List<String>> subTokens = new ArrayList<List<String>>();
        int comboVal = Math.min(tokenList.size(), combo);
        for (int i = 0; i < tokenList.size() - comboVal + 1; i++)
        {
            List<String> subList = processTokens(tokenList.subList(i, i + comboVal));
            if (dropOnNullToken && subList.size() == 0)
            {
                continue;
            }
            if (sortTokens)
            {
                Collections.sort(subList);
            }
            subTokens.add(subList);
        }
        return subTokens;
    }

    private List<String> processTokens(List<String> strings)
    {
        List<String> cleanTokens = new ArrayList<String>();
        for (String token : strings)
        {
            if (token != null)
            {
                token = token.trim();
                Matcher m = badDataPattern.matcher(token);
                if (!token.equals("") && !m.matches())
                {
                    if (forceLowerCase)
                    {
                        token = token.toLowerCase();
                    }
                    if (dropStopWords)
                    {
                        if (ENGLISH_STOP_WORDS_SET.contains(token))
                        {
                            continue;
                        }
                    }
                    cleanTokens.add(token);
                }
            }
        }
        return cleanTokens;
    }

}