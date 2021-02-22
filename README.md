# team-hyacinth-p1

# Malware Classification

This project uses data from the Microsoft Malware Classification Challenge, which consists of nearly half a terabyte of uncompressed data. There are 9 classes of malware, and each instance of malware has one, and only one, category.

We built a Random Forest classifier which achieves an accuracy of 66.41%.

Getting Started
The following instructions will assist you get this project running on your local machine for developing and testing purpose.

Prerequisites:
. Apache Spark 2.3.2
. Python 3.7.2
. Anaconda

Running the tests:
Execute the random forest classifier. The data is automatically imported from the link.

$ python random_forest.py large

Dataset large for large dataset

Features - byte count: 10

Number of trees - 40

Depth - 23

The prediction will be saved to disk in the current directory and named result.txt.

# Authors

(Ordered alphabetically)

+ Aishwarya
+ Ankit
+ Divya

License
This project is licensed under the MIT License
