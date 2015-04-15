from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    file = sc.textFile("gs://gilmarj-demo-hadoop/input/")
    counts = file.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    #counts.saveAsTextFile("gs://gilmarj-demo-hadoop/output")
    counts.map(lambda c: c[0]+','+str(c[1])).saveAsTextFile("gs://gilmarj-demo-hadoop/output")
    sc.stop()
