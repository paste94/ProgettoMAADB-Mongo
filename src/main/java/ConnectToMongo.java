import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.CircleBackground;
import com.kennycason.kumo.font.scale.SqrtFontScalar;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.palette.ColorPalette;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.MapReduceAction;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;

public class ConnectToMongo {
    String shards = "mongodb://localhost:27000";



    private Block<Document> printBlock = new Block<Document>() {
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };

    /*
    {   lemma: "frowing",
        sentiment: {
            sentimentName: "Anger",
            sentimentID: 1
        },
        EmoSN: 0,
        NRC: 1,
        sentisense: 0
    },...
     */

    public void saveLexicalResource(List<LexicalResource> words) {
        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");
        MongoCollection collection = database.getCollection("lexicalresource");

        Map<Integer, String> sentMap = SentimentEnum.getMap();

        sentMap.forEach((num, sent)-> {
            List<Document> documents = new ArrayList<>();
            List<LexicalResource> lexRes = words.stream().filter(e-> e.getSentimentIdFk().equals(num)).collect(Collectors.toList());

            lexRes.forEach(w->{
                documents.add(new Document("lemma", w.getWord())
                        .append("sentiment", new Document("sentName", sent).append("sentID", num))
                        .append("EmoSN", w.getEmosnFreq())
                        .append("NRC", w.getNrcFreq())
                        .append("sentisense", w.getSentisenseFreq()));
            });
            collection.insertMany(documents);
        });

        /*
        sentMap.forEach((num, sent)-> {
            List<Document> documents = new ArrayList<>();
            List<LexicalResource> lexRes = words.stream().filter(e-> e.getSentimentIdFk().equals(num)).collect(Collectors.toList());

            lexRes.forEach(w->{
                documents.add(new Document("lemma", w.getWord())
                                    .append("sentiment", new Document("sentName", sent).append("sentID", num))
                                    .append("EmoSN", w.getEmosnFreq())
                                    .append("NRC", w.getNrcFreq())
                                    .append("sentisense", w.getSentisenseFreq())
                                    .append("", "word"));
            });
            collection.insertMany(documents);
        });

         */
        mongoClient.close();
    }

    public void mapReduceTweet(List<String> tweets){

    }

    public void addLexRes(List<String> listOfWords, int sentiment) {
        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");
        MongoCollection collection = database.getCollection("lexicalresource");
        String sentimentName = SentimentEnum.idToString(sentiment);

        List<Document> documents = new ArrayList<>();

        listOfWords.forEach(w->{
            if(w.length()>0) {
                //{lemma:"prison", "sentiment.sentID": 1}
                Document ins = new Document("lemma", w).append("sentiment", new Document("sentID", sentiment).append("sentimentName", sentimentName));
                documents.add(ins);
            }
        });

        collection.insertMany(documents);
        mongoClient.close();
    }

    public void addLexRes(int threshold, boolean hashtag) {

    }

    public void deleteTable(String tableName) {
        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");
        MongoCollection collection = database.getCollection(tableName);
        collection.deleteMany(new BasicDBObject());
        System.out.println("DROPPED");
        mongoClient.close();
    }

    public void printWordClouds(int sentiment, String fileName) {

    }


    public void mapReduce(){

        final String mapFunction =  "function(){" +
                                    "    emit({'lemma' : this.lemma,'sentiment':this.sentiment.sentID}, 1);" +
                                    "}";

        final String reduceFunction =   "function(k,val){" +
                                        "  return Array.sum( val );" +
                                        "}";



        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");
        MongoCollection collection = database.getCollection("lexicalresource");
        MapReduceIterable iterable = collection.mapReduce(mapFunction, reduceFunction).collectionName("reduced").action(MapReduceAction.REPLACE);
        iterable.toCollection();

        mongoClient.close();
    }

    public void mapReduceEmoji(){

        final String mapFunction =  "function(){" +
                "    emit({'lemma' : this.lemma,'sentiment':this.sentiment.sentID}, 1);" +
                "}";

        final String reduceFunction =   "function(k,val){" +
                "  return Array.sum( val );" +
                "}";



        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");
        MongoCollection collection = database.getCollection("emoji");
        System.out.println(1);
        MapReduceIterable iterable = collection.mapReduce(mapFunction, reduceFunction).collectionName("reducedemoji").action(MapReduceAction.REPLACE).sort(new Document("value", 1));
        iterable.toCollection();
        System.out.println(2);
        mongoClient.close();
    }

    public void mapReducehashtag(){

        final String mapFunction =  "function(){" +
                "    emit({'lemma' : this.lemma,'sentiment':this.sentiment.sentID}, 1);" +
                "}";

        final String reduceFunction =   "function(k,val){" +
                "  return Array.sum( val );" +
                "}";



        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");
        MongoCollection collection = database.getCollection("hashtag");
        System.out.println(1);
        MapReduceIterable iterable = collection.mapReduce(mapFunction, reduceFunction).collectionName("reducedhashtag").action(MapReduceAction.REPLACE).sort(new Document("value", 1));
        iterable.toCollection();
        System.out.println(2);
        mongoClient.close();
    }

    public void mapReduceEmoticon(){

        final String mapFunction =  "function(){" +
                "    emit({'lemma' : this.lemma,'sentiment':this.sentiment.sentID}, 1);" +
                "}";

        final String reduceFunction =   "function(k,val){" +
                "  return Array.sum( val );" +
                "}";



        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");
        MongoCollection collection = database.getCollection("emoticon");
        System.out.println(1);
        MapReduceIterable iterable = collection.mapReduce(mapFunction, reduceFunction).collectionName("reducedemoticon").action(MapReduceAction.REPLACE).sort(new Document("value", 1));
        iterable.toCollection();
        System.out.println(2);
        mongoClient.close();
    }

    public void printWordCloudMongo(int numberOfWords){
        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");
        MongoCollection collection = database.getCollection("reduced");

        FindIterable found = collection.find();

        List<WordFrequency> wordFrequencies = new ArrayList<>();

        while (found.iterator().hasNext()){
            Document d = (Document) found.iterator().next();
        }

        mongoClient.close();
    }

    /*
        {
            emoji: ":)"
            sentiment: {..}
            frequence: 2
        }
     */
    public void addEmojis(List<String> emojis, Integer sentiment) {
        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");

        String sentimentName = SentimentEnum.idToString(sentiment);
        MongoCollection collection = database.getCollection("emoji");

        emojis.forEach(w->{
            Document ins = new Document("lemma", w).append("sentiment", new Document("sentID", sentiment).append("sentimentName", sentimentName));

            collection.insertOne(ins);
        });
        mongoClient.close();
    }

    public void addEmoticon(List<String> emoticons, Integer sentiment) {
        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");

        String sentimentName = SentimentEnum.idToString(sentiment);
        MongoCollection collection = database.getCollection("emoticon");

        emoticons.forEach(w->{
            Document ins = new Document("lemma", w).append("sentiment", new Document("sentID", sentiment).append("sentimentName", sentimentName));

            collection.insertOne(ins);
        });
        mongoClient.close();

    }

    public void addHashtags(List<String> hashtags, Integer sentiment) {
        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");

        String sentimentName = SentimentEnum.idToString(sentiment);
        MongoCollection collection = database.getCollection("hashtag");

        hashtags.forEach(w->{
            Document ins = new Document("lemma", w).append("sentiment", new Document("sentID", sentiment).append("sentimentName", sentimentName));

            collection.insertOne(ins);
        });
        mongoClient.close();
    }

    public void printCloud(int sentimentId, String tableName, int threshold) {

        String fileName = tableName + "_" + SentimentEnum.idToString(sentimentId) + ".png";
        MongoClient mongoClient = MongoClients.create(shards);
        MongoDatabase database = mongoClient.getDatabase("ProgettoMAADB");
        MongoCollection collection = database.getCollection(tableName);

        FindIterable<Document> findIterable = collection.find(new Document("_id.sentiment", sentimentId)).sort(new Document("value", -1)).limit(threshold);

        List<WordFrequency> wordFrequencies = new ArrayList<>();

        for(Document d:findIterable){
            Document id = (Document) d .get("_id");
            String lemma = id.getString("lemma");
            System.out.println(lemma + " -- " + id);
            //System.out.println(lemma + "---" + d.getDouble("value").intValue());
            wordFrequencies.add(new WordFrequency(lemma, d.getDouble("value").intValue()));
        }

        /*

        final Dimension dimension = new Dimension(400, 400);
        final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
        wordCloud.setPadding(2);
        wordCloud.setBackground(new CircleBackground(200));
        wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
        wordCloud.build(wordFrequencies);
        wordCloud.writeToFile("./src/main/resources/word_clouds/" + fileName);
        System.out.println("Generato file " + fileName + ".png");


         */

        final FrequencyAnalyzer frequencyAnalyzer = new FrequencyAnalyzer();
        //final List<WordFrequency> wordFrequencies = frequencyAnalyzer.load("text/my_text_file.txt");
        final Dimension dimension = new Dimension(600, 600);
        final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
        wordCloud.setPadding(2);
        wordCloud.setBackground(new CircleBackground(300));
        wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
        wordCloud.setFontScalar(new SqrtFontScalar(10, 40));
        wordCloud.build(wordFrequencies);
        wordCloud.writeToFile("./src/main/resources/word_clouds/" + fileName);

        mongoClient.close();


    }



}
