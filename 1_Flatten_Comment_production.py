"""
flatten_production.py
=====================
Production script: flatten ทุก subreddit → all_posts / all_comments
Output: gs://reddit-ai-2/flattened_data/all_posts/
        gs://reddit-ai-2/flattened_data/all_comments/
Partition by: subreddit (เพื่อ query แยก subreddit ได้เร็ว)

Submit:
  gcloud dataproc batches submit pyspark \
    gs://reddit-ai-2/scripts/flatten_production.py \
    --project=bigdata-reddit-491813 \
    --region=asia-southeast1 \
    --deps-bucket=gs://dataproc-staging-asia-southeast1-162651271472-konm4ana \
    --properties spark.executor.cores=4,spark.executor.memory=16g,spark.driver.memory=8g,spark.sql.shuffle.partitions=400
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ╔══════════════════════════════════════════════════════════════╗
# ║                        CONFIG                                ║
# ╚══════════════════════════════════════════════════════════════╝

BUCKET       = "gs://reddit-ai-2"
INPUT_PREFIX = f"{BUCKET}/raw_data"
OUTPUT_POSTS    = f"{BUCKET}/flattened_data/all_posts"
OUTPUT_COMMENTS = f"{BUCKET}/flattened_data/all_comments"

# รายชื่อ subreddits ตาม filename จริงใน bucket
# (ตรวจสอบจาก raw_data/ ในภาพ Cloud Storage)
SUBREDDITS = [
    "r_Anthropic",
    "r_ArtificialInteligence",   # ชื่อไฟล์จริง: misspelled (single l)
    "r_Bard",
    "r_ChatGPTPro",
    "r_ChatGPT",
    "r_ClaudeAI",
    "r_DeepSeek",
    "r_GeminiAI",
    "r_OpenAI",
    "r_artificial",
    "r_grok",
    "r_singularity",
]

# ════════════════════════════════════════════════════════════════


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def sc(df: DataFrame, col: str):
    """safe_col: คืน column ถ้ามี ไม่งั้นคืน null"""
    if col in df.columns:
        return F.col(col)
    return F.lit(None).cast(StringType()).alias(col)


def cast_str(df: DataFrame, col: str, alias: str):
    """cast complex field เป็น string — ถ้าไม่มี column คืน null"""
    if col in df.columns:
        return F.col(col).cast(StringType()).alias(alias)
    return F.lit(None).cast(StringType()).alias(alias)


# ──────────────────────────────────────────────────────────────
# flatten_posts — 97 scalar + 17 complex = 114 columns
# ──────────────────────────────────────────────────────────────
def flatten_posts(df: DataFrame) -> DataFrame:
    return df.select(
        sc(df, "id"),
        sc(df, "name"),
        sc(df, "title"),
        sc(df, "selftext"),
        sc(df, "url"),
        sc(df, "url_overridden_by_dest"),
        sc(df, "permalink"),
        sc(df, "domain"),
        sc(df, "post_hint"),
        sc(df, "thumbnail"),
        sc(df, "thumbnail_width"),
        sc(df, "thumbnail_height"),
        sc(df, "author"),
        sc(df, "author_fullname"),
        sc(df, "author_flair_type"),
        sc(df, "author_flair_text"),
        sc(df, "author_flair_text_color"),
        sc(df, "author_flair_background_color"),
        sc(df, "author_flair_template_id"),
        sc(df, "author_flair_css_class"),
        sc(df, "author_cakeday"),
        sc(df, "author_is_blocked"),
        sc(df, "author_patreon_flair"),
        sc(df, "author_premium"),
        sc(df, "link_flair_text"),
        sc(df, "link_flair_type"),
        sc(df, "link_flair_text_color"),
        sc(df, "link_flair_background_color"),
        sc(df, "link_flair_template_id"),
        sc(df, "link_flair_css_class"),
        sc(df, "score"),
        sc(df, "ups"),
        sc(df, "downs"),
        sc(df, "upvote_ratio"),
        sc(df, "num_comments"),
        sc(df, "num_crossposts"),
        sc(df, "gilded"),
        sc(df, "total_awards_received"),
        sc(df, "top_awarded_type"),
        sc(df, "view_count"),
        sc(df, "created_utc"),
        sc(df, "created"),
        sc(df, "edited"),
        sc(df, "retrieved_on"),
        sc(df, "subreddit"),
        sc(df, "subreddit_id"),
        sc(df, "subreddit_name_prefixed"),
        sc(df, "subreddit_type"),
        sc(df, "subreddit_subscribers"),
        sc(df, "suggested_sort"),
        sc(df, "quarantine"),
        sc(df, "pwls"),
        sc(df, "wls"),
        sc(df, "is_self"),
        sc(df, "is_video"),
        sc(df, "is_gallery"),
        sc(df, "is_meta"),
        sc(df, "is_original_content"),
        sc(df, "is_reddit_media_domain"),
        sc(df, "is_robot_indexable"),
        sc(df, "is_crosspostable"),
        sc(df, "is_created_from_ads_ui"),
        sc(df, "over_18"),
        sc(df, "spoiler"),
        sc(df, "stickied"),
        sc(df, "pinned"),
        sc(df, "locked"),
        sc(df, "archived"),
        sc(df, "hidden"),
        sc(df, "hide_score"),
        sc(df, "contest_mode"),
        sc(df, "media_only"),
        sc(df, "allow_live_comments"),
        sc(df, "no_follow"),
        sc(df, "send_replies"),
        sc(df, "saved"),
        sc(df, "clicked"),
        sc(df, "visited"),
        sc(df, "likes"),
        sc(df, "discussion_type"),
        sc(df, "distinguished"),
        sc(df, "can_gild"),
        sc(df, "can_mod_post"),
        sc(df, "category"),
        sc(df, "content_categories"),
        sc(df, "approved_at_utc"),
        sc(df, "approved_by"),
        sc(df, "banned_at_utc"),
        sc(df, "banned_by"),
        sc(df, "removal_reason"),
        sc(df, "removed_by"),
        sc(df, "removed_by_category"),
        sc(df, "num_reports"),
        sc(df, "report_reasons"),
        sc(df, "mod_note"),
        sc(df, "mod_reason_by"),
        sc(df, "mod_reason_title"),

        cast_str(df, "_meta",                 "_meta__json"),
        cast_str(df, "gildings",              "gildings__json"),
        cast_str(df, "all_awardings",         "all_awardings__json"),
        cast_str(df, "awarders",              "awarders__json"),
        cast_str(df, "author_flair_richtext", "author_flair_richtext__json"),
        cast_str(df, "link_flair_richtext",   "link_flair_richtext__json"),
        cast_str(df, "gallery_data",          "gallery_data__json"),
        cast_str(df, "media",                 "media__json"),
        cast_str(df, "media_embed",           "media_embed__json"),
        cast_str(df, "media_metadata",        "media_metadata__json"),
        cast_str(df, "poll_data",             "poll_data__json"),
        cast_str(df, "preview",               "preview__json"),
        cast_str(df, "secure_media",          "secure_media__json"),
        cast_str(df, "secure_media_embed",    "secure_media_embed__json"),
        cast_str(df, "mod_reports",           "mod_reports__json"),
        cast_str(df, "user_reports",          "user_reports__json"),
        cast_str(df, "treatment_tags",        "treatment_tags__json"),
    )


# ──────────────────────────────────────────────────────────────
# flatten_comments — 64 scalar + 9 complex = 73 columns
# ──────────────────────────────────────────────────────────────
def flatten_comments(df: DataFrame) -> DataFrame:
    return df.select(
        sc(df, "id"),
        sc(df, "name"),
        sc(df, "body"),
        sc(df, "author"),
        sc(df, "author_fullname"),
        sc(df, "author_flair_type"),
        sc(df, "author_flair_text"),
        sc(df, "author_flair_text_color"),
        sc(df, "author_flair_background_color"),
        sc(df, "author_flair_template_id"),
        sc(df, "author_flair_css_class"),
        sc(df, "author_cakeday"),
        sc(df, "author_is_blocked"),
        sc(df, "author_patreon_flair"),
        sc(df, "author_premium"),
        sc(df, "score"),
        sc(df, "ups"),
        sc(df, "downs"),
        sc(df, "controversiality"),
        sc(df, "gilded"),
        sc(df, "total_awards_received"),
        sc(df, "top_awarded_type"),
        sc(df, "created_utc"),
        sc(df, "created"),
        sc(df, "edited"),
        sc(df, "retrieved_on"),
        sc(df, "subreddit"),
        sc(df, "subreddit_id"),
        sc(df, "subreddit_name_prefixed"),
        sc(df, "subreddit_type"),
        sc(df, "link_id"),
        sc(df, "parent_id"),
        sc(df, "permalink"),
        sc(df, "replies"),
        sc(df, "score_hidden"),
        sc(df, "stickied"),
        sc(df, "locked"),
        sc(df, "archived"),
        sc(df, "collapsed"),
        sc(df, "collapsed_reason"),
        sc(df, "collapsed_reason_code"),
        sc(df, "collapsed_because_crowd_control"),
        sc(df, "comment_type"),
        sc(df, "distinguished"),
        sc(df, "is_submitter"),
        sc(df, "editable"),
        sc(df, "can_gild"),
        sc(df, "can_mod_post"),
        sc(df, "no_follow"),
        sc(df, "send_replies"),
        sc(df, "saved"),
        sc(df, "likes"),
        sc(df, "unrepliable_reason"),
        sc(df, "approved_at_utc"),
        sc(df, "approved_by"),
        sc(df, "banned_at_utc"),
        sc(df, "banned_by"),
        sc(df, "removal_reason"),
        sc(df, "num_reports"),
        sc(df, "report_reasons"),
        sc(df, "mod_note"),
        sc(df, "mod_reason_by"),
        sc(df, "mod_reason_title"),
        sc(df, "associated_award"),

        cast_str(df, "_meta",                 "_meta__json"),
        cast_str(df, "gildings",              "gildings__json"),
        cast_str(df, "all_awardings",         "all_awardings__json"),
        cast_str(df, "awarders",              "awarders__json"),
        cast_str(df, "author_flair_richtext", "author_flair_richtext__json"),
        cast_str(df, "media_metadata",        "media_metadata__json"),
        cast_str(df, "mod_reports",           "mod_reports__json"),
        cast_str(df, "user_reports",          "user_reports__json"),
        cast_str(df, "treatment_tags",        "treatment_tags__json"),
    )


# ──────────────────────────────────────────────────────────────
# process_type: อ่าน → flatten → write ทีละ subreddit (ไม่ union ใน memory)
# ──────────────────────────────────────────────────────────────
def process_type(spark: SparkSession, file_type: str, output_path: str):
    suffix     = f"_{file_type}.jsonl"
    flatten_fn = flatten_posts if file_type == "posts" else flatten_comments

    print(f"\n{'='*60}")
    print(f"  Processing : {file_type.upper()}")
    print(f"  Output     : {output_path}")
    print(f"  Strategy   : write-per-subreddit (no union in memory)")
    print(f"{'='*60}")

    summary = []

    for sub in SUBREDDITS:
        path    = f"{INPUT_PREFIX}/{sub}{suffix}"
        out_sub = f"{output_path}/subreddit={sub}"
        print(f"\n  [{sub}]")
        print(f"    input  : {path}")
        print(f"    output : {out_sub}")

        try:
            df_raw = (spark.read
                      .option("samplingRatio", "0.01")
                      .option("inferTimestamp", "false")
                      .json(path))

            print(f"    raw cols : {len(df_raw.columns)}")

            df_flat = flatten_fn(df_raw)
            df_flat = (df_flat
                       .withColumn("_source_subreddit", F.lit(sub))
                       .withColumn("_source_file",      F.input_file_name()))

            df_flat.write.mode("overwrite").parquet(out_sub)

            print(f"    status   : OK → {out_sub}")
            summary.append({"subreddit": sub, "status": "OK"})

        except Exception as e:
            print(f"    ERROR    : {e}")
            summary.append({"subreddit": sub, "status": f"ERROR: {str(e)[:120]}"})

        spark.catalog.clearCache()

    print(f"\n  {'─'*55}")
    print(f"  {'Subreddit':<35} Status")
    print(f"  {'─'*55}")
    ok_count = err_count = 0
    for s in summary:
        icon = "✓" if s["status"] == "OK" else "✗"
        print(f"  {icon} {s['subreddit']:<33} {s['status']}")
        if s["status"] == "OK": ok_count  += 1
        else:                   err_count += 1
    print(f"  {'─'*55}")
    print(f"  Done: {ok_count} OK, {err_count} failed")
    print(f"  Output: {output_path}/subreddit=*/")
    print(f"\n  Done writing {file_type}!")


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  REDDIT FLATTEN — PRODUCTION")
    print(f"  Input  : {INPUT_PREFIX}/")
    print(f"  Posts  : {OUTPUT_POSTS}")
    print(f"  Comments: {OUTPUT_COMMENTS}")
    print(f"  Subreddits ({len(SUBREDDITS)}): {', '.join(SUBREDDITS)}")
    print("=" * 60)

    spark = (SparkSession.builder
             .appName("RedditFlattenPosts")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.rpc.askTimeout", "600s")
             .config("spark.network.timeout", "600s")
             .config("spark.executor.heartbeatInterval", "60s")
             .config("spark.sql.jsonGenerator.ignoreNullFields", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    process_type(spark, "posts", OUTPUT_POSTS)

    print("\n" + "=" * 60)
    print("  ALL DONE")
    print(f"  Posts → {OUTPUT_POSTS}")
    print("=" * 60)

    spark.stop()
