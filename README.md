# Project Document: Exploring Reddit Users' Attitudes Towards Generative AI
**Course:** Big Data Computing (2603572) 
**Present date:** May 1, 2026 

## 1. Project Members
* **Kasidit Khampuch** (Project Manager\Data Generalist) 
* **Tanapoom Suwan** (Data Scientist) 
* **Tidarak Jaijit** (Data Scientist\Data Analyst) 
* **Patchakanan Jangsuk** (Data Engineer\Data Analyst) 
* **Andra Hayeechema** (Data Engineer\Data Scientist) 

## 2. Introduction & Problem Statement
Generative AI is typically evaluated through quantitative metrics like usage and market share, but these fail to reflect actual user sentiment. The main challenge is that user perception on social media platforms is complex, unstructured, and often contradictory, making it difficult to analyze. This project focuses on leveraging Big Data processes to analyze large-scale text data from Reddit to deeply understand users' true attitudes towards various Generative AI tools.

## 3. Infrastructure & Tools
* **Data Source:** Raw data collected from Reddit (Dataset: arctic_shift) totaling approximately 18GB. [https://github.com/ArthurHeitmann/arctic_shift](https://github.com/ArthurHeitmann/arctic_shift)
* **Data Storage:** Hosted on Google Cloud Storage within a bucket named `reddit-ai-2`, cleanly partitioned into directories: `raw_data/`, `flattened_data/`, `process_data/`, and `code/`.
* **Processing Infrastructure:** Google Cloud Dataproc Cluster located in the asia-southeast1 region. The cluster utilizes an e2-standard-2 machine type for the master node and two e2-standard-4 machines for the worker nodes.
* **Analytics Platform:** Built using PySpark and Jupyter Notebooks to enable distributed data processing at scale.

## 4. Data Pipeline
The workflow is divided into four main phases representing the end-to-end data pipeline:

### Phase 1: Data Ingestion & Exploratory Data Analysis (EDA)
* Raw data is downloaded and flattened into the Parquet format to simplify distributed processing using the `1_Flatten_Comment_production.py` and `2_Flatten_Post_production.py` scripts.
* Initial EDA, dropping missing values, and data sampling are performed in `3_ReadParquet_DropMissing_Sampling_for_EDA.ipynb`.

### Phase 2: Text Cleaning & Tagging
* Text data undergoes rigorous cleaning, which includes converting text to lowercase, removing length outliers (above the 75th percentile), removing URLs/GIFs, and standardizing timestamps to ISO 8601 format. This is executed via `5_CleanPost_TagAI_new.ipynb` and `6_CleanComment_TagAI_new.ipynb`.
* AI Tagging Methodology is applied using a Defined Keyword List (e.g., ChatGPT, Claude, Gemini) and a binary, case-insensitive keyword matching logic. A fallback logic assigns labels based on the subreddit category if no direct keyword is found.

### Phase 3: Data Labeling & Modeling
* **Sentiment Labeling (Pseudo-labeling):** Since the raw Reddit dataset lacks ground-truth sentiment labels, model comparisons are conducted in `8_Model comparison + Ensemble.ipynb` and `A2_Test_OpenAI_4omini_vs_5-4nano.ipynb` to select the best text annotator. Candidates included BERTweet, GPT-4o-Mini, GPT-5.4-nano, and an Ensemble approach. Ultimately, RoBERTa (cardiffnlp) was selected for data labeling because it delivered comparable scores while offering the lowest inference cost and fastest throughput.
* To identify the most effective approach, various models including Logistic Regression (LR), Naive Bayes (NB), and Support Vector Machine (SVM) were rigorously tested as detailed in `A1 Model Comparison LR vs NB vs SVM.ipynb`. Based on the comparison results, the LinearSVC + OneVsRest architecture was selected for final Big Data deployment.
* The newly labeled dataset is then used to train the main PySpark MLlib model, LinearSVC + OneVsRest, deployed over worker nodes using a feature pipeline consisting of TF-IDF and Word2Vec (50d) in `9_best_model_svc.ipynb`. 

**Performance Metrics**
* **Posts Model:** Accuracy: 0.8040, F1-Score: 0.8000 
* **Comments Model:** Accuracy: 0.7108, F1-Score: 0.7092

### Phase 4: Topic Clustering & Final Analysis
* Topic Extraction targets specific topics using a fast Binary Text Search spanning four groups: Use Cases, Product, User Behavior, and Sentiment.
* Topic Clustering (K-Means) groups post and comment themes in the clustering notebooks (`11_Clustering_Post.ipynb`, `12_Clustering_Comments.ipynb`, `A3_post-clustering-byTitleClean.ipynb` and `A4_comment-clustering-byTitleClean.ipynb`).
* Final result summarization and visualizations are generated via `10_BasicAnalysis_optimized.ipynb`.

## 5. Analysis Results
* **Market Share by Mentions (AI Name Proportion):** ChatGPT remains the undeniable market leader, representing 73.8% of mentions in comments and 61.7% in posts. Claude follows at 10.6% in comments and Gemini at 7.8%.
* **Sentiment Proportion:** The vast majority of user sentiment leans Neutral (54.6% in comments, 71.0% in posts). However, when expressing polarized opinions, Negative sentiment significantly outweighs Positive sentiment (e.g., 27.9% negative vs. 17.5% positive in comments).
* **Dominant Conversation Topics:** The most discussed topics involve code (22.4% in posts) and research (18.2% in posts).
* **Brand Specifics:** Users utilize tools differently. Claude is highly praised and commonly associated with coding and data-heavy tasks. Grok focuses heavily on creative work (like image generation). ChatGPT and Gemini see broader, general discussions that are not tied to any single theme.

## 6. Conclusion & Economic Implications
* **Market Consolidation:** The Generative AI market is steadily consolidating into a Big Three—ChatGPT, Claude, and Gemini—driven by dominant mindshare on platforms like Reddit and rapid user adoption.
* **The Satisfaction Gap:** The higher proportion of negative feedback reflects a growing gap between user expectations and the actual performance of these AI models. Consumers face common pain points across all platforms.
* **Distinct Product Benefits:** Every AI tool offers its own distinct benefits. Users exhibit distinct platform preferences, often favoring Claude for data-heavy tasks while preferring ChatGPT for creative writing.

## 7. Appendix: File References
* **Data Preparation & EDA:** `1_Flatten_Comment_production.py`, `2_Flatten_Post_production.py`, `3_ReadParquet_DropMissing_Sampling_for_EDA.ipynb`, `4_BasicEDA.ipynb`
* **Text Processing & Tagging:** `5_CleanPost_TagAI_new.ipynb`, `6_CleanComment_TagAI_new.ipynb`, `7_Binary_TextSearch.ipynb`
* **Labeling & Machine Learning:** `8_Model comparison + Ensemble.ipynb`, `9_best_model_svc.ipynb`, `A1 Model Comparison LR vs NB vs SVM.ipynb`, `A2_Test_OpenAI_4omini_vs_5-4nano.ipynb`
* **Analysis & Clustering:** `10_BasicAnalysis_optimized.ipynb`, `11_Clustering_Post.ipynb`, `12_Clustering_Comments.ipynb`, `A3_post-clustering-byTitleClean.ipynb`, `A4_comment-clustering-byTitleClean.ipynb`
