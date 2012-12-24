Augury
======

This is a colleciton of scripts that are designed to predict future trends based on word frequencies.  For the blog post describing these tools, please go here:
[http://dannyneil.com/2012/12/02/augury-motivation/](http://dannyneil.com/2012/12/02/augury-motivation/)

## augur.sql
SQL file to be used in conjunction with Hive and Amazon MapReduce to produce word frequencies and growth based on the Google 1-gram dataset.  With 15 small spot instances, this takes about 3 hours to run and produces a ~110 Megabyte summary file.

## augur\_all.rb
Ruby script that does essentially the same thing as "augur.sql" - produce a year-by-year output file with every term that is above the minimum threshold usage as well as its normalized count and year-over-year growth.  You'll also need to download the 2009 Google 1-gram dataset (English).  On my laptop, it takes about 110 minutes to populate the redis database then about 30 minutes to extract the key terms out of it.

Example usage:
redis-server
ruby augur\_all.rb

## augur\_find.rb
Find the top X terms for every year, averaged by Y years.
One direction to use the augury tools: take the year-over-year usages and pay attention to multi-year trends to produce a less noisy and more meaningful dataset.  Run this tool after generating a growth input file (either with augur\_all.rb or augur.sql).  This takes about an hour to run on my laptop.

## augur\_wiki\_compare.rb
Find the highest-scoring term in a list of choices over the given years and summarized by their Wikipedia articles.
For example, you can determine which profession is the fastest-growing profession in terms of word usage by taking the terms, adding in additional terms from their respective Wikipedia articles (e.g., "Apple" => "Apple, touchscreen, iPod, iPhone, Jobs, ...")  Feel free to speed this up and turn it into a game.  It was surprisingly fun to guess which late talk show host would win 1995-2005, or whether "KFC" or "Taco Bell" is more talked about in the 2000s.

## company\_proc.rb
Preprocess the crunchcrawl database to throw out most of the information we don't care about and write out the information that we do care about.  Designed to be used with augur\_companies.rb to correlate growth scores with funding.

## augur\_companies.rb
Load in the company datafile created by company\_proc.rb and the key terms growth file produce by augur.sql to correlate startup funding with key term growth.  End result is a not-very-strong Pearson correlation coefficient of 0.03.  However there are many reasons why that might be, so feel free to tweak parameters to get that higher.
