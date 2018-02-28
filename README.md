# read-big-file-with-spark-scala
The third of my three part case study on how I got on reading a big file with C, Python, spark-python and this - spark-scala

As a reminder, I'm trying to read the same big file (21 Gbytes) we read before with C, python, spark-python but 
this time using spark-scala. It's not a true test as we are only running this on my local PC not on a proper 
cluster. Just thought it would be interesting to try it out. Just to recap, the data file is about 21 Gigabtyes long and holds approximately 335 Million pipe separated records. The first 10 records are shown below:


```
18511|1|2587198|2004-03-31|0|100000|0|1.97|0.49988|100000||||
18511|2|2587198|2004-06-30|0|160000|0|3.2|0.79669|60000|60|||
18511|3|2587198|2004-09-30|0|160000|0|2.17|0.79279|0|0|||
18511|4|2587198|2004-09-30|0|160000|0|1.72|0.79118|0|0|||
18511|5|2587198|2005-03-31|0|0|0|0|0|-160000|-100|||19
18511|6|2587940|2004-03-31|0|240000|0|0.78|0.27327|240000||||
18511|7|2587940|2004-06-30|0|560000|0|1.59|0.63576|320000|133.33||24|
18511|8|2587940|2004-09-30|0|560000|0|1.13|0.50704|0|0|||
18511|9|2587940|2004-09-30|0|560000|0|0.96|0.50704|0|0|||
18511|10|2587940|2005-03-31|0|0|0|0|0|-560000|-100|||14

```

The second field in the above file can range between 1 and 56 and the goal was to split up the original 
file so that all the records with the same value for the second field would be grouped together in the same file. i.e we 
would end up with 56 separate files, period1.txt, period2.txt ... period56.txt each containing approximately 6 million records.

I ran this on a Windows 7 PC with 16Gbytes of ram using scala version 2.1 and scala IDE build of eclipse.I used the same "big file" as was used in my other repositories - read-big-file-with-python and read-big-file-with-spark-python.

The job took 48 minutes to complete but bear in mind there would still have to be a bit of post processing to be done to 
collect all the disparate files together. This compares with the 18 minutes it took to process the same file using just 
python 3.6 on the same PC, 36 minutes using spark-python on the same PC and a whopping the 54 minutes it 
took a C program to process it on an HP Alpha box
