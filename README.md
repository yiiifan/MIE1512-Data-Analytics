# Effect of Versatility on Github Team Productivity

## Overview

This is the final project for MIE1512 Data Analytics.

The purpose of the project is to analyze the effect of diversity of individual members on GitHub team productivity by **Spark**.

The core paper to develop this project on is **Diversity of editors and teams versus quality of cooperative work: experiments on Wikipedia** [1].

> The paper presents some empirical study towards understanding of the role of diversity in individual and whole teams on the quality of the article in open collaboration environment like Wikipedia. In this paper, M. Sydow et al proposed an original diversity measure to quantify the diversity of interests of editor in Wikipedia. The interest profile of each editor is defined as the interest distribution vector over the set of all categories. And the diversity of interests(or equivalently versatility) of the editor is defined as the entropy of interest profile.

Team diversity is one of the fundamental issues in social and organizational studies that has been broadly researched on. Open Source Software(OSS) projects on GitHub, like Wikipedia, is highly rely on collaboration and naturally embraced the diversity. It is interesting to study whether team members have diverse interests tend to be more productive in GitHub projects.



## Dependency

- Jupyter Notebook 4.4.0
- Collected and filtered data by using [GHTorrent ](http://ghtorrent.org/)on Google BigQuery
- Spark Python API [pyspark](<https://spark.apache.org/docs/latest/api/python/index.html>) 



## Content

- Executable Jupyter file [**Link**](<https://github.com/yiiifan/MIE1512-Data-Analytics/blob/master/Final%20Project/ZHANG_YFIAN_VERSATILITY%20ON%20PRODUCTIVITY.ipynb>)
  - [projects.csv](<https://github.com/yiiifan/MIE1512-Data-Analytics/blob/master/Final%20Project/projects.csv>) :  dataset selected is ght_2018_04_01 from ghtorrent-bq. The table contains 83624114 records of GitHub repositories, and the tables we use in this project are projects, users, project_members, project_commits. Since some of Github repositories are not suitable for our analyzation, so I filter the projects by following standards. After filtering, we only select 59244 projects and saved as projects.csv.
  - [project_proccessed.csv](<https://github.com/yiiifan/MIE1512-Data-Analytics/blob/master/Final%20Project/project_processed.csv>) : use Latent Dirichlet Allocation(LDA) to infer the domain of the projects and generate 10 domains for GitHub repositories based on the name and description of the GitHub repositories.
  - [projects_vers.csv](<https://github.com/yiiifan/MIE1512-Data-Analytics/blob/master/Final%20Project/project_vers.csv>)
  - [users_vers.csv](<https://github.com/yiiifan/MIE1512-Data-Analytics/blob/master/Final%20Project/users_vers.csv>)
  - [mem_commits.csv](<https://github.com/yiiifan/MIE1512-Data-Analytics/blob/master/Final%20Project/mem_commits.csv>)
- Final report documentation [**Link**](<https://github.com/yiiifan/MIE1512-Data-Analytics/blob/master/Final%20Project/MIE1512%20Data%20Analytics%20Project%20Submission%20Guidelines-1.pdf>)
- PPT for presentation [**Link**]()

- Spark cheatsheet [**Link**](<https://github.com/yiiifan/MIE1512-Data-Analytics/blob/master/README.md>)

