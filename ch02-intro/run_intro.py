from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

hdfs_root = sys.argv[1]
hdfs_path = sys.argv[2]


def main():
    spark = SparkSession.builder\
        .appName("Intro")\
        .getOrCreate()

    hdfs_root = "hdfs://localhost:9000"
    hdfs_path = "/user/cameres/linkage"
    preview = spark.read.csv(hdfs_root + hdfs_path)
    preview.show()

    for struct_type in preview.schema:
        print struct_type

    parsed = spark.read\
        .option("header", "true")\
        .option("nullValue", "?")\
        .option("inferSchema", "true")\
        .csv(hdfs_root + hdfs_path)
    parsed.show()
    schema = parsed.schema
    for struct_type in schema:
        print struct_type

    parsed.count()
    parsed.cache()
    parsed.groupBy(parsed.is_match)\
        .count()\
        .orderBy(desc("count"))\
        .show()

    parsed.createOrReplaceTempView("linkage")
    spark.sql("""
        SELECT is_match, COUNT(*) cnt
        FROM linkage
        GROUP BY is_match
        ORDER BY cnt DESC
    """).show()

    summary = parsed.describe()
    summary.show()
    summary.select("summary", "cmp_fname_c1", "cmp_fname_c2").show()

    matches = parsed.where("is_match = true")
    misses = parsed.filter(parsed.is_match is False)
    match_summary = matches.describe()
    miss_summary = misses.describe()

    match_summary_t = pivot_summary(match_summary)
    miss_summary_t = pivot_summary(miss_summary)

    match_summary_t.createOrReplaceTempView("match_desc")
    miss_summary_t.createOrReplaceTempView("miss_desc")

    spark.sql("""
        SELECT a.field,
        a.count + b.count total,
        a.mean - b.mean delta
        FROM match_desc a INNER JOIN miss_desc b
        ON a.field = b.field
        ORDER BY delta DESC, total DESC
    """).show()

    # python doesn't support DataSet API
    match_data = parsed
    scored = match_data.rdd\
        .map(lambda md: (score_match_data(md), md.is_match))\
        .toDF(["score", "is_match"])

    cross_tabs(scored, 4.0).show()


def cross_tabs(scored, t):
    return scored\
        .selectExpr("score >= {} as above".format(t), "is_match")\
        .groupBy("above")\
        .pivot("is_match", ["true", "false"])\
        .count()


def score_match_data(md):
    cmp_lname_c1 = md.cmp_lname_c1 if md.cmp_lname_c1 else 0
    cmp_plz = md.cmp_plz if md.cmp_plz else 0
    cmp_by = md.cmp_by if md.cmp_by else 0
    cmp_bm = md.cmp_bm if md.cmp_bm else 0

    return cmp_lname_c1 + cmp_plz + cmp_by + cmp_bm


def pivot_summary(desc):
    lf = long_form(desc)
    return lf.groupBy("field")\
        .pivot("metric", ["count", "mean", "stddev", "min", "max"])\
        .agg(first("value"))


def long_form(desc):
    def flat_map_helper(row):
        metric = row[0]
        return [(metric, schema[i].name, float(row[i]))
                for i in range(1, len(row))]

    schema = desc.schema
    return desc.rdd.flatMap(flat_map_helper).toDF(["metric", "field", "value"])


if __name__ == '__main__':
    main()
