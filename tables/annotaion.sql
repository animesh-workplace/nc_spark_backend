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


CREATE TABLE nc_spark.scores_hg19_normalized_stats
(
    -- Identity columns
    `chr`                       LowCardinality(String),
    `pos`                       UInt32,
    `ref`                       LowCardinality(String),
    `alt`                       LowCardinality(String),

    -- Raw scores
    `GPN`                       Nullable(Float32),
    `GERP`                      Nullable(Float32),
    `NCER`                      Nullable(Float32),
    `DANN`                      Nullable(Float32),
    `REPLISEQ_S2`               Nullable(Float32),
    `REPLISEQ_G1B`              Nullable(Float32),
    `REPLISEQ_S4`               Nullable(Float32),
    `REPLISEQ_S1`               Nullable(Float32),
    `REPLISEQ_G2`               Nullable(Float32),
    `REPLISEQ_S3`               Nullable(Float32),
    `FATHMM_MKL_CODING`         Nullable(Float32),
    `FATHMM_MKL_NONCODING`      Nullable(Float32),
    `ORION`                     Nullable(Float32),
    `CSCAPE_NONCODING`          Nullable(Float32),
    `CSCAPE_CODING`             Nullable(Float32),
    `CADD`                      Nullable(Float32),
    `PhyloP_100way`             Nullable(Float32),
    `PhyloP_30way`              Nullable(Float32),
    `LINSIGHT`                  Nullable(Float32),
    `JARVIS`                    Nullable(Float32),
    `REMM`                      Nullable(Float32),
    `FIRE`                      Nullable(Float32),
    `FUNSEQ2`                   Nullable(Float32),
    `FATHMM_XF_NONCODING`       Nullable(Float32),
    `FATHMM_XF_CODING`          Nullable(Float32),
    `MACIE_REGULATORY`          Nullable(Float32),
    `MACIE_CONSERVED`           Nullable(Float32),
    `REGULOMEDB`                Nullable(Float32),
    `GWRVIS`                    Nullable(Float32),

    -- Pathogenicity group stats
    `pathogenicity_mean`        Nullable(Float32),
    `pathogenicity_median`      Nullable(Float32),
    `pathogenicity_min`         Nullable(Float32),
    `pathogenicity_max`         Nullable(Float32),

    -- Regulatory group stats
    `regulatory_mean`           Nullable(Float32),
    `regulatory_median`         Nullable(Float32),
    `regulatory_min`            Nullable(Float32),
    `regulatory_max`            Nullable(Float32),

    -- Conservation group stats
    `conservation_mean`         Nullable(Float32),
    `conservation_median`       Nullable(Float32),
    `conservation_min`          Nullable(Float32),
    `conservation_max`          Nullable(Float32),

    -- Replication timing group stats
    `replication_timing_mean`   Nullable(Float32),
    `replication_timing_median` Nullable(Float32),
    `replication_timing_min`    Nullable(Float32),
    `replication_timing_max`    Nullable(Float32),

    -- Trinucleotide context
    `trinucleotide`             LowCardinality(String),
)
ENGINE = MergeTree()
PARTITION BY chr
ORDER BY (chr, pos, ref, alt);


CREATE TABLE nc_spark.scores_hg38_normalized_stats
(
    -- Identity columns
    `chr`                       LowCardinality(String),
    `pos`                       UInt32,
    `ref`                       LowCardinality(String),
    `alt`                       LowCardinality(String),

    -- Raw scores
    `GPN`                       Nullable(Float32),
    `GERP`                      Nullable(Float32),
    `NCER`                      Nullable(Float32),
    `DANN`                      Nullable(Float32),
    `REPLISEQ_S2`               Nullable(Float32),
    `REPLISEQ_G1B`              Nullable(Float32),
    `REPLISEQ_S4`               Nullable(Float32),
    `REPLISEQ_S1`               Nullable(Float32),
    `REPLISEQ_G2`               Nullable(Float32),
    `REPLISEQ_S3`               Nullable(Float32),
    `FATHMM_MKL_CODING`         Nullable(Float32),
    `FATHMM_MKL_NONCODING`      Nullable(Float32),
    `ORION`                     Nullable(Float32),
    `CSCAPE_NONCODING`          Nullable(Float32),
    `CSCAPE_CODING`             Nullable(Float32),
    `CADD`                      Nullable(Float32),
    `PhyloP_100way`             Nullable(Float32),
    `PhyloP_30way`              Nullable(Float32),
    `LINSIGHT`                  Nullable(Float32),
    `JARVIS`                    Nullable(Float32),
    `REMM`                      Nullable(Float32),
    `FIRE`                      Nullable(Float32),
    `FUNSEQ2`                   Nullable(Float32),
    `FATHMM_XF_NONCODING`       Nullable(Float32),
    `FATHMM_XF_CODING`          Nullable(Float32),
    `MACIE_REGULATORY`          Nullable(Float32),
    `MACIE_CONSERVED`           Nullable(Float32),
    `REGULOMEDB`                Nullable(Float32),
    `GWRVIS`                    Nullable(Float32),

    -- Pathogenicity group stats
    `pathogenicity_mean`        Nullable(Float32),
    `pathogenicity_median`      Nullable(Float32),
    `pathogenicity_min`         Nullable(Float32),
    `pathogenicity_max`         Nullable(Float32),

    -- Regulatory group stats
    `regulatory_mean`           Nullable(Float32),
    `regulatory_median`         Nullable(Float32),
    `regulatory_min`            Nullable(Float32),
    `regulatory_max`            Nullable(Float32),

    -- Conservation group stats
    `conservation_mean`         Nullable(Float32),
    `conservation_median`       Nullable(Float32),
    `conservation_min`          Nullable(Float32),
    `conservation_max`          Nullable(Float32),

    -- Replication timing group stats
    `replication_timing_mean`   Nullable(Float32),
    `replication_timing_median` Nullable(Float32),
    `replication_timing_min`    Nullable(Float32),
    `replication_timing_max`    Nullable(Float32),

    -- Trinucleotide context
    `trinucleotide`             LowCardinality(String),
)
ENGINE = MergeTree()
PARTITION BY chr
ORDER BY (chr, pos, ref, alt);


ALTER TABLE nc_spark.scores_hg19_normalized_stats
    ADD INDEX idx_variant_bloom
    cityHash64(chr, pos, ref, alt)
    TYPE bloom_filter(0.01)   -- 1% false positive rate
    GRANULARITY 1;

ALTER TABLE nc_spark.scores_hg19_normalized_stats


DROP TABLE IF EXISTS nc_spark.user_results;

CREATE TABLE nc_spark.user_results
(
    -- Session
    `session_id`                  String,

    -- Identity
    `chr`                         LowCardinality(String),
    `pos`                         UInt32,
    `ref`                         LowCardinality(String),
    `alt`                         LowCardinality(String),

    -- Raw scores
    `GPN`                         Nullable(Float32),
    `GERP`                        Nullable(Float32),
    `NCER`                        Nullable(Float32),
    `DANN`                        Nullable(Float32),
    `REPLISEQ_S2`                 Nullable(Float32),
    `REPLISEQ_G1B`                Nullable(Float32),
    `REPLISEQ_S4`                 Nullable(Float32),
    `REPLISEQ_S1`                 Nullable(Float32),
    `REPLISEQ_G2`                 Nullable(Float32),
    `REPLISEQ_S3`                 Nullable(Float32),
    `FATHMM_MKL_CODING`           Nullable(Float32),
    `FATHMM_MKL_NONCODING`        Nullable(Float32),
    `ORION`                       Nullable(Float32),
    `CSCAPE_NONCODING`            Nullable(Float32),
    `CSCAPE_CODING`               Nullable(Float32),
    `CADD`                        Nullable(Float32),
    `PhyloP_100way`               Nullable(Float32),
    `PhyloP_30way`                Nullable(Float32),
    `LINSIGHT`                    Nullable(Float32),
    `JARVIS`                      Nullable(Float32),
    `REMM`                        Nullable(Float32),
    `FIRE`                        Nullable(Float32),
    `FUNSEQ2`                     Nullable(Float32),
    `FATHMM_XF_NONCODING`         Nullable(Float32),
    `FATHMM_XF_CODING`            Nullable(Float32),
    `MACIE_REGULATORY`            Nullable(Float32),
    `MACIE_CONSERVED`             Nullable(Float32),
    `REGULOMEDB`                  Nullable(Float32),
    `GWRVIS`                      Nullable(Float32),

    -- Pathogenicity group stats
    `pathogenicity_mean`          Nullable(Float32),
    `pathogenicity_median`        Nullable(Float32),
    `pathogenicity_min`           Nullable(Float32),
    `pathogenicity_max`           Nullable(Float32),

    -- Regulatory group stats
    `regulatory_mean`             Nullable(Float32),
    `regulatory_median`           Nullable(Float32),
    `regulatory_min`              Nullable(Float32),
    `regulatory_max`              Nullable(Float32),

    -- Conservation group stats
    `conservation_mean`           Nullable(Float32),
    `conservation_median`         Nullable(Float32),
    `conservation_min`            Nullable(Float32),
    `conservation_max`            Nullable(Float32),

    -- Replication timing group stats
    `replication_timing_mean`     Nullable(Float32),
    `replication_timing_median`   Nullable(Float32),
    `replication_timing_min`      Nullable(Float32),
    `replication_timing_max`      Nullable(Float32),

    -- Trinucleotide context
    `trinucleotide`               LowCardinality(String),

    -- Metadata
    `created_at`                  DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY session_id
ORDER BY (session_id, chr, pos, ref, alt)
TTL created_at + INTERVAL 24 HOUR
SETTINGS min_bytes_for_wide_part = 0;



CREATE TABLE IF NOT EXISTS nc_spark.user_uploads
(
    `session_id`  String,
    `genome`      LowCardinality(String),   -- 'hg19' or 'hg38'
    `chr`         LowCardinality(String),
    `pos`         UInt32,
    `ref`         LowCardinality(String),
    `alt`         LowCardinality(String),
    `created_at`  DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (session_id, chr, pos, ref, alt)
TTL created_at + INTERVAL 1 DAY
SETTINGS index_granularity = 8192;


ALTER TABLE nc_spark.user_results
    ADD COLUMN `gene_if_overlapping`  LowCardinality(String) DEFAULT '',
    ADD COLUMN `nearest_gene_plus`    LowCardinality(String) DEFAULT '',
    ADD COLUMN `plus_distance`        Nullable(Int32)        DEFAULT NULL,
    ADD COLUMN `nearest_gene_minus`   LowCardinality(String) DEFAULT '',
    ADD COLUMN `minus_distance`       Nullable(Int32)        DEFAULT NULL;


CREATE TABLE IF NOT EXISTS nc_spark.nearest_gene_hg19
(
    `chr`                  LowCardinality(String),
    `pos`                  UInt32,
    `gene_if_overlapping`  LowCardinality(String),   -- empty string if no overlap
    `nearest_gene_plus`    LowCardinality(String),   -- nearest gene on + strand
    `plus_distance`        Int32,                    -- distance in bp, 0 if overlapping
    `nearest_gene_minus`   LowCardinality(String),   -- nearest gene on - strand
    `minus_distance`       Int32                     -- distance in bp, 0 if overlapping
)
ENGINE = MergeTree
PARTITION BY chr
ORDER BY (chr, pos)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS nc_spark.nearest_gene_hg38
(
    `chr`                  LowCardinality(String),
    `pos`                  UInt32,
    `gene_if_overlapping`  LowCardinality(String),
    `nearest_gene_plus`    LowCardinality(String),
    `plus_distance`        Int32,
    `nearest_gene_minus`   LowCardinality(String),
    `minus_distance`       Int32
)
ENGINE = MergeTree
PARTITION BY chr
ORDER BY (chr, pos)
SETTINGS index_granularity = 8192;
