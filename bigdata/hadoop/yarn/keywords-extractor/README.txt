=========
Functionality
=========

Reads input file line by line, crawls URLs and creates output files with second column populated with top 10 keywords found in crawled htmls.

=========
Building
=========
mvn package -Dmaven.test.skip=true


=========
Usage
=========
yarn jar keywords-extractor.jar <input_file> <output_dir> <numberOfContainers>

Input file must exist in HDFS
Output directory must exist.

=========
Notes
=========
Implemented features:
1) Client starts ApplicationMaster on one of the nodes
2) ApplicationMaster allocates a number of containers. Number of containers is equal or less than number of containers
   provided as a third parameter.
3) Input file is logically split into several parts depending on amount of containers requested.
   However if input file is very small the number of containers can be reduced based on total file size.
4) Containers crawl the provided URLs and each generates an output file with keywords.

Not implemented:
1) Containers memory settings are hard-coded within application. Should be processed as parameters.
2) JVM settings for containers are hard-coded within application. Should be processed as parameters.
3) Input parameters are not validated. If input file does not exist the application just crashes with FileNotFound exception.
4) Application writes it's own jars to HDFS. They probably should be cleaned up after application finishes.
5) Requested web-service is not implemented.
6) Requested resources re-allocation based on cluster topology is not implemented.
7) Case when several input files exist is not supported.