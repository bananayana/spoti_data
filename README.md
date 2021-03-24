# spoti_data

## Collect the data

1. Request your data from spotify.
2. Wait up to 30 days.
3. Download the archive and unzip it into `data/` folder.
4. Run `python scripts/download_features.py` to get more info about tracks and to get their audio features.

## Exploratory analysis
Data profiling and visualization are implemented in [initial_review.ipynb](initial_review.ipynb).  
Profiling report [initial_report.html](initial_report.html).

## Clustering by spotify audio features
Notebook with clustering analysis [clustering.ipynb](clustering.ipynb).