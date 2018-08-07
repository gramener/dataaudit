# dataaudit
Data quality audit tool

## Usage
From command prompt / terminal
```
python dataaudit.py </path/to/CSVfile>
```

From python shell
```
> from dataaudit import runaudits
> runaudits('</path/to/CSVfile>')
```

Example
Auditing the data file from a URL
```
In [1]: import pandas as pd

In [2]: from dataaudit import run_audits

In [3]: run_audits('https://bit.ly/2MpMliR')
https://bit.ly/2MpMliR
========Data Untyped==============
========Column Untyped============
=======Column Typed=================
Outliers count in retweets_count column(numeric):             252
Outliers count in favorite_count column(numeric):             211
Outliers count in hashtags_count column(numeric):             660
Outliers count in favorite_count column(categorical): 2360
Outliers count in lang column(categorical): 29
Outliers count in hashtags_count column(categorical): 6
Outliers count in source column(categorical): 5
Out[3]: 'Audits sucessfully done'
```