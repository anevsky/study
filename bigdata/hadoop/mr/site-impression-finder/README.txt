=========
Functionality
=========

Sorts records of incoming dataset by PinYouId then by Timestamp.
Prints PinYouId with the maximal site-impression (StreamId = 1).
N.B. PinYouId can have value "null" and this value is NOT ignored within calculations.

=========
Building
=========
mvn package -Dmaven.test.skip=true

N.B. Java version is explicitly set to 1.8 in pom.xml. In case you want to build the application with some other JDK please change java.version.

=========
Usage
=========
yarn jar site-impression-finder <input_dir> <output_dir>

Input directory is expected to contain dataset(s) for processing
Format:
Bid ID\tTimestamp\tiPinyou ID\tUser-Agent\tIP(*)\tRegion\tCity\tAd Exchange\tDomain\tURL\tAnonymous URL ID\tAd slot ID\tAd slot width\tAd slot height\tAd slot visibility\tAd slot format\tPaying price\tCreative ID\tBidding price\tAdvertiser ID\tUser Tags\tStream ID

Output directory must not exist.

