CREATE TABLE nc_spark.scores_hg19_normalized
(
    `chr` LowCardinality(String),
    `pos` UInt32,
    `ref` LowCardinality(String),
    `alt` LowCardinality(String),
    `GPN` Nullable(Float32),
    `GERP` Nullable(Float32),
    `NCER` Nullable(Float32),
    `DANN` Nullable(Float32),
    `REPLISEQ_S2` Nullable(Float32),
    `REPLISEQ_G1B` Nullable(Float32),
    `REPLISEQ_S4` Nullable(Float32),
    `REPLISEQ_S1` Nullable(Float32),
    `REPLISEQ_G2` Nullable(Float32),
    `REPLISEQ_S3` Nullable(Float32),
    `FATHMM_MKL_CODING` Nullable(Float32),
    `FATHMM_MKL_NONCODING` Nullable(Float32),
    `ORION` Nullable(Float32),
    `CSCAPE_NONCODING` Nullable(Float32),
    `CSCAPE_CODING` Nullable(Float32),
    `CADD` Nullable(Float32),
    `PhyloP_100way` Nullable(Float32),
    `PhyloP_30way` Nullable(Float32),
    `LINSIGHT` Nullable(Float32),
    `JARVIS` Nullable(Float32),
    `REMM` Nullable(Float32),
    `FIRE` Nullable(Float32),
    `FUNSEQ2` Nullable(Float32),
    `FATHMM_XF_NONCODING` Nullable(Float32),
    `FATHMM_XF_CODING` Nullable(Float32),
    `MACIE_REGULATORY` Nullable(Float32),
    `MACIE_CONSERVED` Nullable(Float32),
    `REGULOMEDB` Nullable(Float32),
    `GWRVIS` Nullable(Float32)
)
ENGINE = MergeTree()
PARTITION BY chr
ORDER BY (chr, pos, ref, alt);

CREATE TABLE nc_spark.scores_hg19_raw
(
    `chr` LowCardinality(String),
    `pos` UInt32,
    `ref` LowCardinality(String),
    `alt` LowCardinality(String),
    `GPN` Nullable(Float32),
    `GERP` Nullable(Float32),
    `NCER` Nullable(Float32),
    `DANN` Nullable(Float32),
    `REPLISEQ_S2` Nullable(Float32),
    `REPLISEQ_G1B` Nullable(Float32),
    `REPLISEQ_S4` Nullable(Float32),
    `REPLISEQ_S1` Nullable(Float32),
    `REPLISEQ_G2` Nullable(Float32),
    `REPLISEQ_S3` Nullable(Float32),
    `FATHMM_MKL_CODING` Nullable(Float32),
    `FATHMM_MKL_NONCODING` Nullable(Float32),
    `ORION` Nullable(Float32),
    `CSCAPE_NONCODING` Nullable(Float32),
    `CSCAPE_CODING` Nullable(Float32),
    `CADD` Nullable(Float32),
    `PhyloP_100way` Nullable(Float32),
    `PhyloP_30way` Nullable(Float32),
    `LINSIGHT` Nullable(Float32),
    `JARVIS` Nullable(Float32),
    `REMM` Nullable(Float32),
    `FIRE` Nullable(Float32),
    `FUNSEQ2` Nullable(Float32),
    `FATHMM_XF_NONCODING` Nullable(Float32),
    `FATHMM_XF_CODING` Nullable(Float32),
    `MACIE_REGULATORY` Nullable(Float32),
    `MACIE_CONSERVED` Nullable(Float32),
    `REGULOMEDB` Nullable(Float32),
    `GWRVIS` Nullable(Float32)
)
ENGINE = MergeTree()
PARTITION BY chr
ORDER BY (chr, pos, ref, alt);

CREATE TABLE nc_spark.scores_hg38_normalized
(
    `chr` LowCardinality(String),
    `pos` UInt32,
    `ref` LowCardinality(String),
    `alt` LowCardinality(String),
    `GPN` Nullable(Float32),
    `GERP` Nullable(Float32),
    `NCER` Nullable(Float32),
    `DANN` Nullable(Float32),
    `REPLISEQ_S2` Nullable(Float32),
    `REPLISEQ_G1B` Nullable(Float32),
    `REPLISEQ_S4` Nullable(Float32),
    `REPLISEQ_S1` Nullable(Float32),
    `REPLISEQ_G2` Nullable(Float32),
    `REPLISEQ_S3` Nullable(Float32),
    `FATHMM_MKL_CODING` Nullable(Float32),
    `FATHMM_MKL_NONCODING` Nullable(Float32),
    `ORION` Nullable(Float32),
    `CSCAPE_NONCODING` Nullable(Float32),
    `CSCAPE_CODING` Nullable(Float32),
    `CADD` Nullable(Float32),
    `PhyloP_100way` Nullable(Float32),
    `PhyloP_30way` Nullable(Float32),
    `LINSIGHT` Nullable(Float32),
    `JARVIS` Nullable(Float32),
    `REMM` Nullable(Float32),
    `FIRE` Nullable(Float32),
    `FUNSEQ2` Nullable(Float32),
    `FATHMM_XF_NONCODING` Nullable(Float32),
    `FATHMM_XF_CODING` Nullable(Float32),
    `MACIE_REGULATORY` Nullable(Float32),
    `MACIE_CONSERVED` Nullable(Float32),
    `REGULOMEDB` Nullable(Float32),
    `GWRVIS` Nullable(Float32)
)
ENGINE = MergeTree()
PARTITION BY chr
ORDER BY (chr, pos, ref, alt);

CREATE TABLE nc_spark.scores_hg38_raw
(
    `chr` LowCardinality(String),
    `pos` UInt32,
    `ref` LowCardinality(String),
    `alt` LowCardinality(String),
    `GPN` Nullable(Float32),
    `GERP` Nullable(Float32),
    `NCER` Nullable(Float32),
    `DANN` Nullable(Float32),
    `REPLISEQ_S2` Nullable(Float32),
    `REPLISEQ_G1B` Nullable(Float32),
    `REPLISEQ_S4` Nullable(Float32),
    `REPLISEQ_S1` Nullable(Float32),
    `REPLISEQ_G2` Nullable(Float32),
    `REPLISEQ_S3` Nullable(Float32),
    `FATHMM_MKL_CODING` Nullable(Float32),
    `FATHMM_MKL_NONCODING` Nullable(Float32),
    `ORION` Nullable(Float32),
    `CSCAPE_NONCODING` Nullable(Float32),
    `CSCAPE_CODING` Nullable(Float32),
    `CADD` Nullable(Float32),
    `PhyloP_100way` Nullable(Float32),
    `PhyloP_30way` Nullable(Float32),
    `LINSIGHT` Nullable(Float32),
    `JARVIS` Nullable(Float32),
    `REMM` Nullable(Float32),
    `FIRE` Nullable(Float32),
    `FUNSEQ2` Nullable(Float32),
    `FATHMM_XF_NONCODING` Nullable(Float32),
    `FATHMM_XF_CODING` Nullable(Float32),
    `MACIE_REGULATORY` Nullable(Float32),
    `MACIE_CONSERVED` Nullable(Float32),
    `REGULOMEDB` Nullable(Float32),
    `GWRVIS` Nullable(Float32)
)
ENGINE = MergeTree()
PARTITION BY chr
ORDER BY (chr, pos, ref, alt);


parallel -j 22 --bar 'mawk "BEGIN{FS=OFS=\"\t\"} {if(NR>1){for(i=5;i<=NF;i++) if(\$i==\"NA\") \$i=\"\\\\N\"} print}" ../hg19/processed/db_chr{}.tsv | ./clickhouse client --query="INSERT INTO nc_spark.scores_hg19_raw FORMAT TSVWithNames"' ::: {1..22}

parallel -j 22 --bar 'mawk "BEGIN{FS=OFS=\"\t\"} {if(NR>1){for(i=5;i<=NF;i++) if(\$i==\"NA\") \$i=\"\\\\N\"} print}" ../hg19/normalized/norm_chr{}.tsv | ./clickhouse client --query="INSERT INTO nc_spark.scores_hg19_normalized FORMAT TSVWithNames"' ::: {1..22}

parallel -j 22 --bar 'mawk "BEGIN{FS=OFS=\"\t\"} {if(NR>1){for(i=5;i<=NF;i++) if(\$i==\"NA\") \$i=\"\\\\N\"} print}" ../hg38/processed/db_chr{}.tsv | ./clickhouse client --query="INSERT INTO nc_spark.scores_hg38_raw FORMAT TSVWithNames"' ::: {1..22} 

parallel -j 22 --bar 'mawk "BEGIN{FS=OFS=\"\t\"} {if(NR>1){for(i=5;i<=NF;i++) if(\$i==\"NA\") \$i=\"\\\\N\"} print}" ../hg38/normalized/norm_chr{}.tsv | ./clickhouse client --query="INSERT INTO nc_spark.scores_hg38_normalized FORMAT TSVWithNames"' ::: {1..22}