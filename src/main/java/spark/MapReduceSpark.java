package spark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import java.util.*;


public class MapReduceSpark {
    public static void main(String[] args) {
        //Reading lines from file - A
//        SparkConf conf = new SparkConf().setAppName("Words Analysis").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> lines = sc.textFile("src/main/resources/test.txt");


        //Reading lines from file -B
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Simple Application")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile("src/main/resources/words.txt").javaRDD();

        //System.out.println("maxWordFrequency");
        //maxWordFrequency(lines);
        System.out.println("wordCount");
        wordCount(lines);
        System.out.println("longestWords");
        longestWords(lines);
        System.out.println("averageWordsLength");
        averageWordLength(lines);
        System.out.println("anagramm");
        anagramm(lines);
        System.out.println("consecutiveWords");
        consecutiveWords(lines);

    }
    // TODO 4.1

    public static void wordCount(JavaRDD<String> lines ){
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line
                        .replaceAll("\\p{P}", " ")
                        .split("\\s+"))
                .iterator()).filter(word -> !word.isEmpty());

        JavaPairRDD<String,Integer> pairs = words.mapToPair(word->new Tuple2<>(word,1));
        JavaPairRDD<String,Integer> wordCounts=pairs.reduceByKey((a,b)->a+b);
        JavaPairRDD<Integer, String> swapped =
                wordCounts.mapToPair(pair->new Tuple2<>(pair._2,pair._1));
        JavaPairRDD<Integer,String> orderedPairs=swapped.sortByKey(false);
        List<Tuple2<Integer, String>> takePairs=orderedPairs.take(10);
        takePairs.forEach(System.out::println);

    }

    public static void longestWords(JavaRDD<String> lines){
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line
                        .replaceAll("\\p{P}", " ")
                        .split("\\s+"))
                .iterator()).filter(word -> !word.isEmpty());
        JavaPairRDD<String,Integer> wordCounts = words.mapToPair(word->new Tuple2<>(word,word.length()))
                .reduceByKey((a,b)->a);
        JavaPairRDD<Integer,String> swapped=wordCounts.mapToPair(word->new Tuple2<>(word._2, word._1))
                .sortByKey(false);
        List<Tuple2<Integer, String>> takePairs=swapped.take(10);
        takePairs.forEach(System.out::println);
    }

    public static void averageWordLength(JavaRDD<String> lines){
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line
                        .replaceAll("\\p{P}", " ")
                        .split("\\s+"))
                .iterator()).filter(word -> !word.isEmpty());

            JavaPairRDD<Integer, Integer> wordCount=words.mapToPair(word->new Tuple2<>(1,word.length()));
            Tuple2<Integer,Integer> totalCount=wordCount
                    .reduce((a,b)->new Tuple2<>(a._1+b._1,a._2+b._2));
            System.out.println((double)totalCount._2/totalCount._1);
    }

    public static void anagramm(JavaRDD<String> lines){
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line
                        .replaceAll("\\p{P}", " ")
                        .split("\\s+"))
                        .iterator()).filter(word -> !word.isEmpty());
        JavaPairRDD<String,String> wordList=words.mapToPair(word->new Tuple2<>(sortWord(word),word));
        JavaPairRDD<String,Iterable<String>> sameWord=wordList.groupByKey();

//       No duplicates
        JavaRDD<List<String>> anagr =
                sameWord.values()
                        .map(group -> {
                            Set<String> set = new HashSet<>();
                            group.forEach(set::add);
                            return (List<String>) new ArrayList<>(set);
                        })
                        .filter(li -> li.size() > 1);
        anagr.collect().forEach(System.out::println);

//        using combineByKey ---> More efficient than groupByKey
//        JavaPairRDD<String, Set<String>> grouped =
//                wordList.combineByKey(
//
//                        // createCombiner
//                        word -> {
//                            Set<String> list = new HashSet<>();
//                            list.add(word);
//                            return list;
//                        },
//
//                        // mergeValue
//                        (list, word) -> {
//                            list.add(word);
//                            return list;
//                        },
//
//                        // mergeCombiners
//                        (list1, list2) -> {
//                            list1.addAll(list2);
//                            return list1;
//                        }
//                ).filter( li -> li._2.size() >1);
//        grouped.collect().forEach(System.out::println);

    }


    private static String sortWord(String word) {
        char[] chars = word.toCharArray();
        Arrays.sort(chars);
        return new String(chars);
    }

    private static void consecutiveWords(JavaRDD<String> lines){
        JavaRDD<String[]> tokenized=lines.map(line -> line
                        .replaceAll("\\p{P}", " ")
                        .trim()
                        .split("\\s+")
        );

        JavaRDD<String> bigrams =
                tokenized.flatMap(words -> {
                    List<String> result = new ArrayList<>();
                    for (int i = 0; i < words.length - 1; i++) {
                        result.add(words[i] + " " + words[i + 1]);
                    }
                    return result.iterator();
                });

        JavaPairRDD<String,Integer> countBigrams=bigrams.mapToPair(word->new Tuple2<>(word,1));
        JavaPairRDD<String,Integer> totalCount=countBigrams.reduceByKey((a,b)->a+b)
                .filter(li->li._2>10);
        totalCount.mapToPair(Tuple2::swap).sortByKey(false).collect().forEach(System.out::println);
    }

}
